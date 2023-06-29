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
	index int64
	// TODO: This could be a simple vector
	contents map[int64]int

	nextWrite int64
}

type Ftl struct {
	blockSize int64
	numBlocks int
	chip      *flashblock.Chip

	blockMap    []int64
	eraseBlocks []*eraseBlockInfo

	currentWriteEraseBlock int64
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
		blockSize:              blockSize,
		numBlocks:              numBlocks,
		chip:                   chip,
		blockMap:               make([]int64, numBlocks),
		eraseBlocks:            make([]*eraseBlockInfo, chip.EraseBlockCount()),
		currentWriteEraseBlock: -1,
	}
	for i := range f.blockMap {
		f.blockMap[i] = -1
	}
	for i := range f.eraseBlocks {
		ebi := &eraseBlockInfo{
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
	return f.freeBlocks.Remove(f.freeBlocks.Front()).(*eraseBlockInfo)
}

func (f *Ftl) fetchWriteBlock() *eraseBlockInfo {
	var eb *eraseBlockInfo
	if f.currentWriteEraseBlock >= 0 {
		eb = f.eraseBlocks[int(f.currentWriteEraseBlock)]
		if eb.nextWrite == f.chip.EraseBlockSize() {
			log.Printf("Filled erase block %d: utilisation %d/%d",
				eb.index, len(eb.contents), f.chip.EraseBlockSize()/f.blockSize)
			eb = nil
		} else if eb.nextWrite > f.chip.EraseBlockSize() {
			log.Fatalf("eraseBlock.nextWrite %d > eraseBlockSize %d",
				eb.nextWrite, f.chip.EraseBlockSize())
		}
	}

	if eb == nil {
		eb = f.fetchEmptyEraseBlock()
		if eb == nil {
			panic("No free erase blocks. TODO: Implement GC")
		}
		f.currentWriteEraseBlock = eb.index
	}

	return eb
}

func (f *Ftl) getCurrentBlock(block int64) *eraseBlockInfo {
	blockIndex := f.blockMap[block]
	if blockIndex < 0 {
		return nil
	}
	return f.eraseBlocks[blockIndex/f.chip.EraseBlockSize()]
}

func (f *Ftl) freeEraseBlockIfEmpty(ebi *eraseBlockInfo) {
	if len(ebi.contents) > 0 {
		return
	}

	log.Println("==== Erase block %d empty, erasing and freeing", ebi.index)
	ebi.nextWrite = 0
	f.chip.EraseBlock(ebi.index)
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
