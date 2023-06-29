package flashblock

type EraseBlock struct {
	blockSize, size int64

	backing ReadWriterAt
}

func NewEraseBlock(blockSize, size int64, backing ReadWriterAt) *EraseBlock {
	b := &EraseBlock{
		blockSize: blockSize,
		size:      size,
		backing:   backing,
	}

	return b
}

func (b *EraseBlock) Erase() error {
	_, err := WriteZero(b.backing, 0, b.size)
	return err
}

func (b *EraseBlock) WriteAt(p []byte, off int64) (int, error) {
	// TODO: Track ranges which have been written and error on overwriting.
	return b.backing.WriteAt(p, off)
}

func (b *EraseBlock) ReadAt(p []byte, off int64) (int, error) {
	return b.backing.ReadAt(p, off)
}
