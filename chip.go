package flashblock

import (
	"sync"
)

type Chip struct {
	eraseBlockSize, size int64

	backing ReadWriterAt
	blocks  []*EraseBlock

	lock sync.Mutex
}

func NewChip(eraseBlockSize, size int64, backing ReadWriterAt) *Chip {
	if size%eraseBlockSize != 0 {
		panic("size MUST be a multiple of eraseBlockSize")
	}

	numBlocks := int(size / eraseBlockSize)

	c := &Chip{
		eraseBlockSize: eraseBlockSize,
		size:           size,
		backing:        backing,
		blocks:         make([]*EraseBlock, numBlocks),
	}

	for i := range c.blocks {
		offset := int64(i) * eraseBlockSize
		c.blocks[i] = NewEraseBlock(
			eraseBlockSize, NewOffsetReadWriterAt(backing, offset))
	}

	return c
}

func (c *Chip) Size() int64 {
	return c.size
}

func (c *Chip) EraseBlockSize() int64 {
	return c.eraseBlockSize
}

func (c *Chip) EraseBlockCount() int {
	return len(c.blocks)
}

func (c *Chip) EraseBlock(index int) error {
	c.lock.Lock()
	defer c.lock.Unlock()

	return c.blocks[index].Erase()
}

func (c *Chip) WriteAtBlock(index int64, p []byte, off int64) (int, error) {
	c.lock.Lock()
	defer c.lock.Unlock()

	return c.blocks[index].WriteAt(p, off)
}

func (c *Chip) ReadAtBlock(index int64, p []byte, off int64) (int, error) {
	c.lock.Lock()
	defer c.lock.Unlock()

	return c.blocks[index].ReadAt(p, off)
}
