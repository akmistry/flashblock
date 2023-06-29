package flashblock

import (
	"sync"
)

type Chip struct {
	blockSize, eraseBlockSize, size int64

	backing ReadWriterAt
	blocks  []*EraseBlock

	lock sync.Mutex
}

func NewChip(blockSize, eraseBlockSize, size int64, backing ReadWriterAt) *Chip {
	if eraseBlockSize%blockSize != 0 {
		panic("eraseBlockSize MUST be a multiple of blockSize")
	} else if size%eraseBlockSize != 0 {
		panic("size MUST be a multiple of eraseBlockSize")
	}

	numBlocks := int(size / eraseBlockSize)

	c := &Chip{
		blockSize:      blockSize,
		eraseBlockSize: eraseBlockSize,
		size:           size,
		backing:        backing,
		blocks:         make([]*EraseBlock, numBlocks),
	}

	for i := range c.blocks {
		offset := int64(i) * eraseBlockSize
		c.blocks[i] = NewEraseBlock(
			blockSize, eraseBlockSize, NewOffsetReadWriterAt(backing, offset))
	}

	return c
}

func (c *Chip) EraseBlockCount() int {
	return len(c.blocks)
}

func (c *Chip) EraseBlock(index int) error {
	c.lock.Lock()
	defer c.lock.Unlock()

	return c.blocks[index].Erase()
}

func (c *Chip) WriteAtBlock(index int, p []byte, off int64) (int, error) {
	c.lock.Lock()
	defer c.lock.Unlock()

	return c.blocks[index].WriteAt(p, off)
}

func (c *Chip) ReadAtBlock(index int, p []byte, off int64) (int, error) {
	c.lock.Lock()
	defer c.lock.Unlock()

	return c.blocks[index].ReadAt(p, off)
}
