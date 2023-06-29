package simpleftl

import (
	"container/list"
	"errors"
	"math/bits"
	"sync"

	"github.com/akmistry/flashblock"
)

var (
	errUnalignedOffset = errors.New("simpleftl: unaligned offset")
	errUnalignedLength = errors.New("simpleftl: unaligned length")
)

type eraseBlockInfo struct {
	// TODO: This could be a simple vector
	contents map[int64]int
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
			contents: make(map[int64]int),
		}
		f.eraseBlocks[i] = ebi
		f.freeBlocks.PushBack(ebi)
	}
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
	eraseBlockIndex := blockIndex / eraseBlockSize
	eraseBlockOffset :=
		(blockIndex - (eraseBlockIndex * eraseBlockSize)) * f.blockSize
	_, err := f.chip.ReadAtBlock(int(eraseBlockIndex), p, eraseBlockOffset)
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

func (f *Ftl) WriteAt(p []byte, off int64) (int, error) {
	if off%f.blockSize != 0 {
		return 0, errUnalignedOffset
	} else if int64(len(p))%f.blockSize != 0 {
		return 0, errUnalignedLength
	}

	f.lock.Lock()
	defer f.lock.Unlock()

	return 0, nil
}

/*
func (f *Ftl) Trim(off int64, length uint32) error {
	if off%f.blockSize != 0 {
		return errUnalignedOffset
	} else if int64(length)%f.blockSize != 0 {
		return errUnalignedLength
	}

	f.lock.Lock()
	defer f.lock.Unlock()

	return nil
}
*/
