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
	open   bool
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

// Open is the primary interface for initializing the bucket on disk and setting
// up for writes.
func (b *Bucket) Open() error {
	b.Lock()
	defer b.Unlock()

	if b.open {
		return errors.New("bucket already open")
	}

	if err := b.create(); err != nil {
		return err
	}

	b.open = true

	return nil
}

// Close flushes everything in memory to disk, converts the bucket to stop
// accepting new writes and seeks the file pointer back to the beginning in
// preparation for reading. (as such, it must be called before being read from)
func (b *Bucket) Close() error {
	b.Lock()
	defer b.Unlock()

	if err := b.flush(); err != nil {
		return err
	}

	b.open = false
	if _, err := b.file.Seek(0, 0); err != nil {
		return err
	}

	return nil
}

func (b *Bucket) create() error {
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
	b.Lock()
	defer b.Unlock()

	if !b.open {
		return errors.New("bucket not accepting writes, make sure to open it first")
	}

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

	return b.flush()
}

func (b *Bucket) flush() error {
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

// Read implements io.Reader for easy interoperability.
func (b *Bucket) Read(p []byte) (int, error) {
	b.RLock()
	defer b.RUnlock()

	if b.open {
		return 0, errors.New("bucket accepting writes, make sure to close before reading")
	}

	return b.file.Read(p)
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
