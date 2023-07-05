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
	sectorOverhead = 4 * 8
)

type eraseBlockInfo struct {
	f     *Ftl
	index int64

	// Vector of (sector, offset+1) pairs. A negative offset indicates
	// the sector has been removed. A zero offset indicates an unused entry.
	activeSectors       int
	activeSectorsOffset int64

	nextWrite int64

	// TODO: Save this in the erase block
	eraseCount int
}

func (i *eraseBlockInfo) full() bool {
	return i.nextWrite >= i.f.eraseBlockCapacity
}

func (i *eraseBlockInfo) erase() {
	i.nextWrite = 0
	i.eraseCount++

	i.activeSectors = 0
	i.activeSectorsOffset = 0

	i.f.chip.EraseBlock(i.index)
}

func (i *eraseBlockInfo) appendSector(sector, offset int64) {
	if offset >= i.f.eraseBlockCapacity {
		log.Printf("ERROR: offset %d >= block size %d", offset, i.f.eraseBlockCapacity)
	}

	var buf [2 * 8]byte
	le.PutUint64(buf[0:], uint64(sector))
	le.PutUint64(buf[8:], uint64(offset+1))
	_, err := i.f.chip.WriteAtBlock(i.index, buf[:], i.f.eraseBlockCapacity+i.activeSectorsOffset)
	if err != nil {
		log.Printf("ERROR: write of active sectors failed: %v", err)
	}
	i.activeSectorsOffset += int64(len(buf))

	i.activeSectors++
}

func (i *eraseBlockInfo) removeSector(sector int64) {
	/*
		if int64(len(i.activeBlockMap)) >= (i.f.sectorsPerEraseBlock * 4) {
			log.Printf("ERROR: blockmap entries %d > expected %d",
				len(i.activeBlockMap), (i.f.sectorsPerEraseBlock * 4))
		}
	*/

	var buf [2 * 8]byte
	offset := int64(-1)
	le.PutUint64(buf[0:], uint64(sector))
	le.PutUint64(buf[8:], uint64(offset))
	_, err := i.f.chip.WriteAtBlock(i.index, buf[:], i.f.eraseBlockCapacity+i.activeSectorsOffset)
	if err != nil {
		log.Printf("ERROR: write of active sectors failed: %v", err)
	}
	i.activeSectorsOffset += int64(len(buf))

	i.activeSectors--

	if i.activeSectors < 0 {
		log.Printf("ERROR: invalid activeSectors: %d", i.activeSectors)
	}
}

func (i *eraseBlockInfo) readActiveBlocks() []int64 {
	bufSize := i.f.chip.EraseBlockSize() - i.f.eraseBlockCapacity
	buf := make([]byte, bufSize)
	_, err := i.f.chip.ReadAtBlock(i.index, buf, i.f.eraseBlockCapacity)
	if err != nil {
		log.Printf("ERROR: read of active sectors failed: %v", err)
	}

	var bs []int64
	for i := 0; i < int(bufSize); i += 16 {
		sector := int64(le.Uint64(buf[i:]))
		offset := int64(le.Uint64(buf[i+8:]))
		bs = append(bs, sector, offset)
	}
	return bs
}

func (i *eraseBlockInfo) readActiveBlocksMap() (map[int64]int64, int) {
	activeSectorsMap := make(map[int64]int64)
	activeSectors := i.readActiveBlocks()
	sectorEntries := 0
	for i := 0; i < len(activeSectors); i += 2 {
		sector := activeSectors[i]
		offset := activeSectors[i+1]
		if offset == 0 {
			// End of entries
			break
		}
		sectorEntries++
		if offset > 0 {
			activeSectorsMap[sector] = offset - 1
		} else {
			delete(activeSectorsMap, sector)
		}
	}
	return activeSectorsMap, sectorEntries
}

type Ftl struct {
	// Size of a block/sector/page
	sectorSize int64
	// Number of sectors
	numBlocks int64

	// Number of "erase blocks"
	numEraseBlocks int64
	// Number of sectors per "erase block". Not necessarily
	// eraseBlockSize/sectorSize.
	sectorsPerEraseBlock int64
	// Number of bytes a "erase block" can hold. May be less than eraseBlockSize.
	eraseBlockCapacity int64

	chip *flashblock.Chip

	sectorMap   []int64
	eraseBlocks []*eraseBlockInfo

	currentWriteEraseBlock *eraseBlockInfo
	freeBlocks             list.List

	lock sync.Mutex
}

func New(sectorSize int64, chip *flashblock.Chip) *Ftl {
	if sectorSize <= 0 {
		panic("sectorSize MUST be positive")
	} else if bits.OnesCount64(uint64(sectorSize)) != 1 {
		panic("sectorSize MUST be a power-of-2")
	} else if chip.EraseBlockSize()%sectorSize != 0 {
		panic("erase block size MUST be a multiple of sectorSize")
	}

	sectorsPerEraseBlock := chip.EraseBlockSize() / sectorSize
	eraseBlockOverhead := sectorsPerEraseBlock * sectorOverhead
	overheadBlocks := (eraseBlockOverhead + sectorSize - 1) / sectorSize
	sectorsPerEraseBlock -= overheadBlocks

	numBlocks := sectorsPerEraseBlock * chip.EraseBlockCount()
	f := &Ftl{
		sectorSize:           sectorSize,
		numBlocks:            numBlocks,
		numEraseBlocks:       chip.EraseBlockCount(),
		sectorsPerEraseBlock: sectorsPerEraseBlock,
		eraseBlockCapacity:   sectorsPerEraseBlock * sectorSize,

		chip:        chip,
		sectorMap:   make([]int64, numBlocks),
		eraseBlocks: make([]*eraseBlockInfo, chip.EraseBlockCount()),
	}
	for i := range f.sectorMap {
		f.sectorMap[i] = -1
	}
	for i := range f.eraseBlocks {
		ebi := &eraseBlockInfo{
			f:     f,
			index: int64(i),
		}
		f.eraseBlocks[i] = ebi

		activeSectors, blockEntries := ebi.readActiveBlocksMap()
		if len(activeSectors) == 0 {
			if blockEntries > 0 {
				// Should have already been erased.
				ebi.erase()
			}
			f.freeBlocks.PushBack(ebi)
		} else {
			ebi.activeSectors = len(activeSectors)
			ebi.activeSectorsOffset = int64(blockEntries) * 16

			log.Printf("Mapping used erase block %d, entries: %d", i, blockEntries)
			log.Printf("Active blocks: %d, active block offset: %d",
				ebi.activeSectors, ebi.activeSectorsOffset)
			for sector, offset := range activeSectors {
				writeSectorIndex := f.eraseBlockOffsetToSectorIndex(int64(i), offset)
				f.sectorMap[sector] = writeSectorIndex
			}
		}
	}
	log.Printf("sector map entries: %d, erase blocks: %d, sectorsPerEraseBlock: %d",
		len(f.sectorMap), len(f.eraseBlocks), f.sectorsPerEraseBlock)

	go f.dumpHist()

	return f
}

func (f *Ftl) Size() int64 {
	return f.sectorSize * int64(len(f.sectorMap))
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
		if b.activeSectors == 0 {
			continue
		}

		hist[b.activeSectors/16]++
	}
	return hist
}

func (f *Ftl) eraseBlockIndexOffset(sector int64) (i int64, off int64) {
	i = sector / f.sectorsPerEraseBlock
	off = (sector % f.sectorsPerEraseBlock) * f.sectorSize
	return
}

func (f *Ftl) eraseBlockOffsetToSectorIndex(eb, off int64) int64 {
	return eb*f.sectorsPerEraseBlock + (off / f.sectorSize)
}

func (f *Ftl) getCurrentSector(sector int64) (*eraseBlockInfo, int64) {
	blockIndex := f.sectorMap[sector]
	if blockIndex < 0 {
		return nil, 0
	}
	ebIndex, ebOffset := f.eraseBlockIndexOffset(blockIndex)
	return f.eraseBlocks[ebIndex], ebOffset
}

func (f *Ftl) readSector(p []byte, sector int64) error {
	if len(p) != int(f.sectorSize) {
		log.Printf("ERROR: read len(p) %d != sectorSize", len(p))
	}

	ebi, off := f.getCurrentSector(sector)
	if ebi == nil {
		// Unallocated sector, fill zeros
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
		if gcEbi == nil || b.activeSectors < lastContentLen {
			gcEbi = b
			lastContentLen = b.activeSectors
		}
	}

	log.Printf("GCing erase block %d, utilisation %d/%d",
		gcEbi.index, lastContentLen, f.sectorsPerEraseBlock)

	buf := make([]byte, f.sectorSize)
	activeSectors, _ := gcEbi.readActiveBlocksMap()
	for sector, offset := range activeSectors {
		_, err := f.chip.ReadAtBlock(gcEbi.index, buf, int64(offset))
		if err != nil {
			log.Printf("ERROR: ReadAtBlock(%d, %d, %d) error: %v",
				gcEbi.index, len(buf), offset, err)
			continue
		}

		err = f.writeSector(buf, sector, ebi.index)
		if err != nil {
			log.Printf("ERROR: writeSector(%d, %d, %d) error: %v",
				len(buf), sector, ebi.index, err)
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
		//	eb.index, len(eb.contents), f.chip.EraseBlockSize()/f.sectorSize)
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
	if ebi.activeSectors > 0 {
		return
	}

	if ebi == f.currentWriteEraseBlock {
		log.Printf("Avoid erasing current erase block %d with usage %d",
			ebi.index, ebi.activeSectors)
		return
	}
	/*
		if !ebi.full() {
			log.Printf("Avoid erasing non-full erase block %d with usage %d",
				ebi.index, ebi.activeSectors)
			return
		}
	*/

	ebi.erase()
	f.freeBlocks.PushBack(ebi)
	//log.Printf("==== Erase block %d empty, erasing and freeing. Free blocks %d",
	//	ebi.index, f.freeBlocks.Len())
}

func (f *Ftl) writeSector(p []byte, sector, eraseBlock int64) error {
	if len(p) != int(f.sectorSize) {
		log.Printf("ERROR: write len(p) %d != sectorSize", len(p))
	}

	ebi := f.eraseBlocks[eraseBlock]
	writeOffset := ebi.nextWrite
	_, err := f.chip.WriteAtBlock(eraseBlock, p[:f.sectorSize], writeOffset)
	if err != nil {
		return err
	}
	ebi.appendSector(sector, writeOffset)
	ebi.nextWrite += f.sectorSize

	writeSectorIndex := f.eraseBlockOffsetToSectorIndex(eraseBlock, writeOffset)
	f.sectorMap[sector] = writeSectorIndex

	return nil
}

func (f *Ftl) ReadAt(p []byte, off int64) (int, error) {
	if off%f.sectorSize != 0 {
		return 0, errUnalignedOffset
	} else if int64(len(p))%f.sectorSize != 0 {
		return 0, errUnalignedLength
	}

	f.lock.Lock()
	defer f.lock.Unlock()

	n := 0
	for ; n < len(p); n += int(f.sectorSize) {
		sector := (off + int64(n)) / f.sectorSize
		err := f.readSector(p[n:n+int(f.sectorSize)], sector)
		if err != nil {
			return n, err
		}
	}

	return n, nil
}

func (f *Ftl) WriteAt(p []byte, off int64) (int, error) {
	if off%f.sectorSize != 0 {
		return 0, errUnalignedOffset
	} else if int64(len(p))%f.sectorSize != 0 {
		return 0, errUnalignedLength
	}

	f.lock.Lock()
	defer f.lock.Unlock()

	n := 0
	for ; n < len(p); n += int(f.sectorSize) {
		sector := (off + int64(n)) / f.sectorSize
		ebi, _ := f.getCurrentSector(sector)
		if ebi != nil {
			ebi.removeSector(sector)
			f.freeEraseBlockIfEmpty(ebi)
		}
		ebi = f.fetchWriteBlock()

		err := f.writeSector(p[n:n+int(f.sectorSize)], sector, ebi.index)
		if err != nil {
			return n, err
		}
	}

	return n, nil
}

func (f *Ftl) Trim(off int64, length uint32) error {
	log.Printf("Trim(off = %d, len = %d)", off, length)

	if off%f.sectorSize != 0 {
		return errUnalignedOffset
	} else if int64(length)%f.sectorSize != 0 {
		return errUnalignedLength
	}

	f.lock.Lock()
	defer f.lock.Unlock()

	for length > 0 {
		sector := off / f.sectorSize
		ebi, _ := f.getCurrentSector(sector)
		if ebi != nil {
			ebi.removeSector(sector)
			f.freeEraseBlockIfEmpty(ebi)
		}
		f.sectorMap[sector] = -1

		length -= uint32(f.sectorSize)
		off += f.sectorSize
	}

	return nil
}
