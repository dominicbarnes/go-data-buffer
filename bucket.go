package buffer

import (
	"bufio"
	"errors"
	"sync"

	"github.com/spf13/afero"
)

// Bucket represents a single data sink.
type Bucket struct {
	sync.RWMutex
	path   string
	fs     afero.Fs
	file   afero.File
	writer *bufio.Writer
	writes uint
	bytes  uint64
}

// NewBucket creates a new bucket instance with the given options.
func NewBucket(o BucketOptions) *Bucket {
	o.defaults()

	return &Bucket{
		path: o.Path,
		fs:   o.Fs,
	}
}

// Create does the initial creation of the underlying file and bufio writer. If
// the file already exists, it will be emptied. This method should only be
// invoked once, and will return an error if invoked multiple times.
func (b *Bucket) Create() error {
	b.Lock()
	defer b.Unlock()

	if b.file != nil {
		return errors.New("bucket already created")
	}

	file, err := b.fs.Create(b.path)
	if err != nil {
		return err
	}

	b.file = file
	b.writer = bufio.NewWriter(file)

	return nil
}

// Destroy removes the file from disk.
func (b *Bucket) Destroy() error {
	b.Lock()
	defer b.Unlock()

	if err := b.fs.Remove(b.file.Name()); err != nil {
		return err
	}

	b.writes = 0
	b.bytes = 0

	return nil
}

// Write adds the given data to this bucket.
func (b *Bucket) Write(data []byte) error {
	if b.file == nil {
		if err := b.Create(); err != nil {
			return err
		}
	}

	b.Lock()
	defer b.Unlock()

	if _, err := b.writer.Write(data); err != nil {
		return err
	}

	b.writes++
	b.bytes += uint64(len(data))

	return nil
}

// Flush ensures that any data held in memory is flushed to disk immediately.
func (b *Bucket) Flush() error {
	b.Lock()
	defer b.Unlock()

	if err := b.writer.Flush(); err != nil {
		return err
	}

	return nil
}

// Writes is used to retrieve the number of writes issued for this bucket.
func (b *Bucket) Writes() uint {
	b.RLock()
	defer b.RUnlock()

	return b.writes
}

// Bytes is used to retrieve the number of bytes written to this bucket.
func (b *Bucket) Bytes() uint64 {
	b.RLock()
	defer b.RUnlock()

	return b.bytes
}

// File returns the underlying file so it can be read from.
func (b *Bucket) File() afero.File {
	return b.file
}

// BucketOptions is used to configure bucket instances.
type BucketOptions struct {
	Path string
	Fs   afero.Fs
}

func (o *BucketOptions) defaults() {
	if o.Fs == nil {
		o.Fs = afero.NewOsFs()
	}
}
