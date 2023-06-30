package simpleftl

import (
	"container/list"
	"errors"
	"log"
	"math/bits"
	"sync"

	"github.com/akmistry/flashblock"
)

var (
	errUnalignedOffset = errors.New("simpleftl: unaligned offset")
	errUnalignedLength = errors.New("simpleftl: unaligned length")
)

type eraseBlockInfo struct {
	f *Ftl

	index int64
	// TODO: This could be a simple vector
	contents map[int64]int

	nextWrite int64
}

func (i *eraseBlockInfo) full() bool {
	return i.nextWrite >= i.f.blockSize
}

func (i *eraseBlockInfo) erase() {
	i.contents = make(map[int64]int)
	i.nextWrite = 0
	i.f.chip.EraseBlock(i.index)
}

type Ftl struct {
	blockSize int64
	numBlocks int
	chip      *flashblock.Chip

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

	numBlocks := int(chip.Size() / blockSize)
	f := &Ftl{
		blockSize:   blockSize,
		numBlocks:   numBlocks,
		chip:        chip,
		blockMap:    make([]int64, numBlocks),
		eraseBlocks: make([]*eraseBlockInfo, chip.EraseBlockCount()),
	}
	for i := range f.blockMap {
		f.blockMap[i] = -1
	}
	for i := range f.eraseBlocks {
		ebi := &eraseBlockInfo{
			f:        f,
			index:    int64(i),
			contents: make(map[int64]int),
		}
		f.eraseBlocks[i] = ebi
		f.freeBlocks.PushBack(ebi)
	}
	log.Printf("block map entries: %d, erase blocks: %d",
		len(f.blockMap), len(f.eraseBlocks))
	return f
}

func (f *Ftl) readBlock(p []byte, block int64) error {
	blockIndex := f.blockMap[block]
	if blockIndex < 0 {
		// Unallocated block, fill zeros
		for i := 0; i < int(f.blockSize); i++ {
			p[i] = 0
		}
		return nil
	}
	eraseBlockSize := f.chip.EraseBlockSize()
	eraseBlockIndex := blockIndex / (eraseBlockSize / f.blockSize)
	eraseBlockOffset :=
		(blockIndex * f.blockSize) - (eraseBlockIndex * eraseBlockSize)
	_, err := f.chip.ReadAtBlock(eraseBlockIndex, p, eraseBlockOffset)
	return err
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
	for len(p) > 0 {
		block := off / f.blockSize
		err := f.readBlock(p[:f.blockSize], block)
		if err != nil {
			return n, err
		}
		p = p[f.blockSize:]
		n += int(f.blockSize)
		off += f.blockSize
	}

	return n, nil
}

func (f *Ftl) fetchEmptyEraseBlock() *eraseBlockInfo {
	if f.freeBlocks.Len() == 0 {
		return nil
	}
	ebi := f.freeBlocks.Remove(f.freeBlocks.Front()).(*eraseBlockInfo)
	if f.freeBlocks.Len() > 0 {
		return ebi
	}
	log.Print("Allocated final free erase block, running GC")

	var gcEbi *eraseBlockInfo
	lastContentLen := 0
	for _, b := range f.eraseBlocks {
		if b == ebi {
			continue
		}
		if gcEbi == nil || len(b.contents) < lastContentLen {
			gcEbi = b
			lastContentLen = len(b.contents)
		}
	}

	log.Printf("GCing erase block %d, utilisation %d/%d",
		gcEbi.index, lastContentLen, f.chip.EraseBlockSize()/f.blockSize)

	buf := make([]byte, f.blockSize)
	eraseBlockSize := f.chip.EraseBlockSize()
	for block, offset := range gcEbi.contents {
		_, err := f.chip.ReadAtBlock(gcEbi.index, buf, int64(offset))
		if err != nil {
			panic(err)
		}

		writeOffset := ebi.nextWrite
		_, err = f.chip.WriteAtBlock(ebi.index, buf, writeOffset)
		if err != nil {
			panic(err)
		}
		ebi.contents[block] = int(writeOffset)

		writeBlockIndex :=
			((ebi.index * eraseBlockSize) + writeOffset) / f.blockSize
		f.blockMap[block] = writeBlockIndex

		ebi.nextWrite += f.blockSize
	}

	gcEbi.erase()
	f.freeBlocks.PushBack(gcEbi)

	return ebi
}

func (f *Ftl) fetchWriteBlock() *eraseBlockInfo {
	eb := f.currentWriteEraseBlock
	if eb != nil {
		if eb.nextWrite == f.chip.EraseBlockSize() {
			//log.Printf("Filled erase block %d: utilisation %d/%d",
			//	eb.index, len(eb.contents), f.chip.EraseBlockSize()/f.blockSize)
			eb = nil
			f.currentWriteEraseBlock = nil
		} else if eb.nextWrite > f.chip.EraseBlockSize() {
			log.Fatalf("eraseBlock.nextWrite %d > eraseBlockSize %d",
				eb.nextWrite, f.chip.EraseBlockSize())
		}
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

func (f *Ftl) getCurrentBlock(block int64) *eraseBlockInfo {
	blockIndex := f.blockMap[block]
	if blockIndex < 0 {
		return nil
	}
	eraseBlockIndex := (blockIndex * f.blockSize) / f.chip.EraseBlockSize()
	return f.eraseBlocks[eraseBlockIndex]
}

func (f *Ftl) freeEraseBlockIfEmpty(ebi *eraseBlockInfo) {
	if len(ebi.contents) > 0 {
		return
	}

	if ebi == f.currentWriteEraseBlock {
		log.Printf("Avoid erasing current erase block %d with usage %d",
			ebi.index, len(ebi.contents))
		return
	}
	if !ebi.full() {
		log.Printf("Avoid erasing non-full erase block %d with usage %d",
			ebi.index, len(ebi.contents))
		return
	}

	log.Printf("==== Erase block %d empty, erasing and freeing", ebi.index)
	ebi.erase()
	f.freeBlocks.PushBack(ebi)
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
	for len(p) > 0 {
		block := off / f.blockSize
		ebi := f.getCurrentBlock(block)
		if ebi != nil {
			delete(ebi.contents, block)
			f.freeEraseBlockIfEmpty(ebi)
			//log.Printf("Relocating block %d from erase block %d, new erase block contents: %d",
			//	block, ebi.index, len(ebi.contents))
		}
		ebi = f.fetchWriteBlock()

		writeOffset := ebi.nextWrite
		ebi.nextWrite += f.blockSize
		_, err := f.chip.WriteAtBlock(ebi.index, p[:f.blockSize], writeOffset)
		if err != nil {
			return n, err
		}
		ebi.contents[block] = int(writeOffset)

		eraseBlockSize := f.chip.EraseBlockSize()
		writeBlockIndex :=
			((ebi.index * eraseBlockSize) + writeOffset) / f.blockSize
		f.blockMap[block] = writeBlockIndex

		p = p[f.blockSize:]
		n += int(f.blockSize)
		off += f.blockSize
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
		ebi := f.getCurrentBlock(block)
		if ebi != nil {
			delete(ebi.contents, block)
			f.freeEraseBlockIfEmpty(ebi)
		}
		f.blockMap[block] = -1
		length -= uint32(f.blockSize)
	}

	return nil
}
