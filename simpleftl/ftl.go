package simpleftl

import (
	"container/list"
	"encoding/binary"
	"errors"
	"log"
	"math/bits"
	"sync"
	"time"

	"github.com/akmistry/flashblock"
)

var (
	errUnalignedOffset = errors.New("simpleftl: unaligned offset")
	errUnalignedLength = errors.New("simpleftl: unaligned length")

	le = binary.LittleEndian
)

const (
	blockOverhead = 4 * 8
)

type eraseBlockInfo struct {
	f     *Ftl
	index int64

	// Vector of (block, offset+1) pairs. A negative offset indicates
	// the block has been removed. A zero offset indicates an unused entry.
	activeBlocks       int
	activeBlocksOffset int64

	nextWrite int64

	eraseCount int
}

func (i *eraseBlockInfo) full() bool {
	return i.nextWrite >= i.f.eraseBlockCapacity
}

func (i *eraseBlockInfo) erase() {
	i.nextWrite = 0
	i.eraseCount++

	i.activeBlocks = 0
	i.activeBlocksOffset = 0

	i.f.chip.EraseBlock(i.index)
}

func (i *eraseBlockInfo) appendBlock(block, offset int64) {
	if offset >= i.f.eraseBlockCapacity {
		log.Printf("ERROR: offset %d >= block size %d", offset, i.f.eraseBlockCapacity)
	}

	var buf [2 * 8]byte
	le.PutUint64(buf[0:], uint64(block))
	le.PutUint64(buf[8:], uint64(offset+1))
	_, err := i.f.chip.WriteAtBlock(i.index, buf[:], i.f.eraseBlockCapacity+i.activeBlocksOffset)
	if err != nil {
		log.Printf("ERROR: write of active blocks failed: %v", err)
	}
	i.activeBlocksOffset += int64(len(buf))

	i.activeBlocks++
}

func (i *eraseBlockInfo) removeBlock(block int64) {
	/*
		if int64(len(i.activeBlockMap)) >= (i.f.blocksPerEraseBlock * 4) {
			log.Printf("ERROR: blockmap entries %d > expected %d",
				len(i.activeBlockMap), (i.f.blocksPerEraseBlock * 4))
		}
	*/

	var buf [2 * 8]byte
	offset := int64(-1)
	le.PutUint64(buf[0:], uint64(block))
	le.PutUint64(buf[8:], uint64(offset))
	_, err := i.f.chip.WriteAtBlock(i.index, buf[:], i.f.eraseBlockCapacity+i.activeBlocksOffset)
	if err != nil {
		log.Printf("ERROR: write of active blocks failed: %v", err)
	}
	i.activeBlocksOffset += int64(len(buf))

	i.activeBlocks--

	if i.activeBlocks < 0 {
		log.Printf("ERROR: invalid activeBlocks: %d", i.activeBlocks)
	}
}

func (i *eraseBlockInfo) readActiveBlocks() []int64 {
	bufSize := i.f.chip.EraseBlockSize() - i.f.eraseBlockCapacity
	buf := make([]byte, bufSize)
	_, err := i.f.chip.ReadAtBlock(i.index, buf, i.f.eraseBlockCapacity)
	if err != nil {
		log.Printf("ERROR: read of active blocks failed: %v", err)
	}

	var bs []int64
	for i := 0; i < int(bufSize); i += 16 {
		block := int64(le.Uint64(buf[i:]))
		offset := int64(le.Uint64(buf[i+8:]))
		bs = append(bs, block, offset)
	}
	return bs
}

type Ftl struct {
	// Size of a block/sector/page
	blockSize int64
	// Number of blocks
	numBlocks int64

	// Number of "erase blocks"
	numEraseBlocks int64
	// Number of blocks per "erase block". Not necessarily
	// eraseBlockSize/blockSize.
	blocksPerEraseBlock int64
	// Number of bytes a "erase block" can hold. May be less than eraseBlockSize.
	eraseBlockCapacity int64

	chip *flashblock.Chip

	blockMap    []int64
	eraseBlocks []*eraseBlockInfo

	currentWriteEraseBlock *eraseBlockInfo
	freeBlocks             list.List

	lock sync.Mutex
}

func New(blockSize int64, chip *flashblock.Chip) *Ftl {
	if blockSize <= 0 {
		panic("blockSize MUST be positive")
	} else if bits.OnesCount64(uint64(blockSize)) != 1 {
		panic("blockSize MUST be a power-of-2")
	} else if chip.EraseBlockSize()%blockSize != 0 {
		panic("erase block size MUST be a multiple of blockSize")
	}

	blocksPerEraseBlock := chip.EraseBlockSize() / blockSize
	eraseBlockOverhead := blocksPerEraseBlock * blockOverhead
	overheadBlocks := (eraseBlockOverhead + blockSize - 1) / blockSize
	blocksPerEraseBlock -= overheadBlocks

	numBlocks := blocksPerEraseBlock * chip.EraseBlockCount()
	f := &Ftl{
		blockSize:           blockSize,
		numBlocks:           numBlocks,
		numEraseBlocks:      chip.EraseBlockCount(),
		blocksPerEraseBlock: blocksPerEraseBlock,
		eraseBlockCapacity:  blocksPerEraseBlock * blockSize,

		chip:        chip,
		blockMap:    make([]int64, numBlocks),
		eraseBlocks: make([]*eraseBlockInfo, chip.EraseBlockCount()),
	}
	for i := range f.blockMap {
		f.blockMap[i] = -1
	}
	for i := range f.eraseBlocks {
		ebi := &eraseBlockInfo{
			f:     f,
			index: int64(i),
		}
		f.eraseBlocks[i] = ebi
		f.freeBlocks.PushBack(ebi)
	}
	log.Printf("block map entries: %d, erase blocks: %d, blocksPerEraseBlock: %d",
		len(f.blockMap), len(f.eraseBlocks), f.blocksPerEraseBlock)

	go f.dumpHist()

	return f
}

func (f *Ftl) Size() int64 {
	return f.blockSize * int64(len(f.blockMap))
}

func (f *Ftl) dumpHist() {
	totalEraseBlocks := len(f.eraseBlocks)

	ticker := time.NewTicker(5 * time.Second)
	for range ticker.C {
		f.lock.Lock()

		log.Printf("Used erase blocks: %d/%d",
			totalEraseBlocks-f.freeBlocks.Len(), totalEraseBlocks)

		h := f.generateEraseCountHist()
		log.Printf("Erase count histogram: %v", h)

		h = f.generateUtilHist()
		log.Printf("Utilisation histogram: %v", h)

		f.lock.Unlock()
	}
}

func (f *Ftl) generateEraseCountHist() []int {
	var hist []int
	for _, b := range f.eraseBlocks {
		if b.eraseCount >= len(hist) {
			hist = append(hist, make([]int, b.eraseCount-len(hist)+1)...)
		}

		hist[b.eraseCount]++
	}
	return hist
}

func (f *Ftl) generateUtilHist() []int {
	hist := make([]int, 17)
	for _, b := range f.eraseBlocks {
		if b.activeBlocks == 0 {
			continue
		}

		hist[b.activeBlocks/16]++
	}
	return hist
}

func (f *Ftl) eraseBlockIndexOffset(block int64) (i int64, off int64) {
	i = block / f.blocksPerEraseBlock
	off = (block % f.blocksPerEraseBlock) * f.blockSize
	return
}

func (f *Ftl) eraseBlockOffsetToBlockIndex(eb, off int64) int64 {
	return eb*f.blocksPerEraseBlock + (off / f.blockSize)
}

func (f *Ftl) getCurrentBlock(block int64) (*eraseBlockInfo, int64) {
	blockIndex := f.blockMap[block]
	if blockIndex < 0 {
		return nil, 0
	}
	ebIndex, ebOffset := f.eraseBlockIndexOffset(blockIndex)
	return f.eraseBlocks[ebIndex], ebOffset
}

func (f *Ftl) readBlock(p []byte, block int64) error {
	if len(p) != int(f.blockSize) {
		log.Printf("ERROR: read len(p) %d != blockSize", len(p))
	}

	ebi, off := f.getCurrentBlock(block)
	if ebi == nil {
		// Unallocated block, fill zeros
		for i := range p {
			p[i] = 0
		}
		return nil
	}

	_, err := f.chip.ReadAtBlock(ebi.index, p, off)
	return err
}

func (f *Ftl) fetchEmptyEraseBlock() *eraseBlockInfo {
	if f.freeBlocks.Len() == 0 {
		return nil
	}
	ebi := f.freeBlocks.Remove(f.freeBlocks.Front()).(*eraseBlockInfo)
	if f.freeBlocks.Len() > 0 {
		return ebi
	}

	var gcEbi *eraseBlockInfo
	lastContentLen := 0
	for _, b := range f.eraseBlocks {
		if b == ebi {
			continue
		}
		if gcEbi == nil || b.activeBlocks < lastContentLen {
			gcEbi = b
			lastContentLen = b.activeBlocks
		}
	}

	log.Printf("GCing erase block %d, utilisation %d/%d",
		gcEbi.index, lastContentLen, f.blocksPerEraseBlock)

	buf := make([]byte, f.blockSize)
	liveBlocks := make(map[int64]int64)
	activeBlocks := gcEbi.readActiveBlocks()
	for i := 0; i < len(activeBlocks); i += 2 {
		block := activeBlocks[i]
		offset := activeBlocks[i+1]
		if offset == 0 {
			// End of entries
			break
		}
		if offset > 0 {
			liveBlocks[block] = offset - 1
		} else {
			delete(liveBlocks, block)
		}
	}
	for block, offset := range liveBlocks {
		_, err := f.chip.ReadAtBlock(gcEbi.index, buf, int64(offset))
		if err != nil {
			log.Printf("ERROR: ReadAtBlock(%d, %d, %d) error: %v",
				gcEbi.index, len(buf), offset, err)
			continue
		}

		err = f.writeBlock(buf, block, ebi.index)
		if err != nil {
			log.Printf("ERROR: writeBlock(%d, %d, %d) error: %v",
				len(buf), block, ebi.index, err)
			continue
		}
	}

	gcEbi.erase()
	f.freeBlocks.PushBack(gcEbi)

	return ebi
}

func (f *Ftl) fetchWriteBlock() *eraseBlockInfo {
	eb := f.currentWriteEraseBlock
	if eb != nil && eb.full() {
		if eb.nextWrite > f.eraseBlockCapacity {
			log.Printf("ERROR: eraseBlock.nextWrite %d > eraseBlockCapacity %d",
				eb.nextWrite, f.eraseBlockCapacity)
		}

		//log.Printf("Filled erase block %d: utilisation %d/%d",
		//	eb.index, len(eb.contents), f.chip.EraseBlockSize()/f.blockSize)
		eb = nil
		f.currentWriteEraseBlock = nil
	}

	if eb == nil {
		eb = f.fetchEmptyEraseBlock()
		if eb == nil {
			panic("No free erase blocks")
		}
		f.currentWriteEraseBlock = eb
	}

	return eb
}

func (f *Ftl) freeEraseBlockIfEmpty(ebi *eraseBlockInfo) {
	if ebi.activeBlocks > 0 {
		return
	}

	if ebi == f.currentWriteEraseBlock {
		log.Printf("Avoid erasing current erase block %d with usage %d",
			ebi.index, ebi.activeBlocks)
		return
	}
	if !ebi.full() {
		log.Printf("Avoid erasing non-full erase block %d with usage %d",
			ebi.index, ebi.activeBlocks)
		return
	}

	ebi.erase()
	f.freeBlocks.PushBack(ebi)
	//log.Printf("==== Erase block %d empty, erasing and freeing. Free blocks %d",
	//	ebi.index, f.freeBlocks.Len())
}

func (f *Ftl) writeBlock(p []byte, block, eraseBlock int64) error {
	if len(p) != int(f.blockSize) {
		log.Printf("ERROR: write len(p) %d != blockSize", len(p))
	}

	ebi := f.eraseBlocks[eraseBlock]
	writeOffset := ebi.nextWrite
	_, err := f.chip.WriteAtBlock(eraseBlock, p[:f.blockSize], writeOffset)
	if err != nil {
		return err
	}
	ebi.appendBlock(block, writeOffset)
	ebi.nextWrite += f.blockSize

	writeBlockIndex := f.eraseBlockOffsetToBlockIndex(eraseBlock, writeOffset)
	f.blockMap[block] = writeBlockIndex

	return nil
}

func (f *Ftl) ReadAt(p []byte, off int64) (int, error) {
	if off%f.blockSize != 0 {
		return 0, errUnalignedOffset
	} else if int64(len(p))%f.blockSize != 0 {
		return 0, errUnalignedLength
	}

	f.lock.Lock()
	defer f.lock.Unlock()

	n := 0
	for ; n < len(p); n += int(f.blockSize) {
		block := (off + int64(n)) / f.blockSize
		err := f.readBlock(p[n:n+int(f.blockSize)], block)
		if err != nil {
			return n, err
		}
	}

	return n, nil
}

func (f *Ftl) WriteAt(p []byte, off int64) (int, error) {
	if off%f.blockSize != 0 {
		return 0, errUnalignedOffset
	} else if int64(len(p))%f.blockSize != 0 {
		return 0, errUnalignedLength
	}

	f.lock.Lock()
	defer f.lock.Unlock()

	n := 0
	for ; n < len(p); n += int(f.blockSize) {
		block := (off + int64(n)) / f.blockSize
		ebi, _ := f.getCurrentBlock(block)
		if ebi != nil {
			ebi.removeBlock(block)
			f.freeEraseBlockIfEmpty(ebi)
		}
		ebi = f.fetchWriteBlock()

		err := f.writeBlock(p[n:n+int(f.blockSize)], block, ebi.index)
		if err != nil {
			return n, err
		}
	}

	return n, nil
}

func (f *Ftl) Trim(off int64, length uint32) error {
	log.Printf("Trim(off = %d, len = %d)", off, length)

	if off%f.blockSize != 0 {
		return errUnalignedOffset
	} else if int64(length)%f.blockSize != 0 {
		return errUnalignedLength
	}

	f.lock.Lock()
	defer f.lock.Unlock()

	for length > 0 {
		block := off / f.blockSize
		ebi, _ := f.getCurrentBlock(block)
		if ebi != nil {
			ebi.removeBlock(block)
			f.freeEraseBlockIfEmpty(ebi)
		}
		f.blockMap[block] = -1

		length -= uint32(f.blockSize)
		off += f.blockSize
	}

	return nil
}
