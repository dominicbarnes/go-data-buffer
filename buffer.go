package buffer

import (
	"path/filepath"
	"sync"

	"github.com/spf13/afero"
)

// Buffer represents a data buffering target.
type Buffer struct {
	sync.RWMutex
	buckets map[string]*Bucket
	root    string
	fs      afero.Fs
}

// NewBuffer creates a new instance from the given options.
func NewBuffer(o BufferOptions) *Buffer {
	o.defaults()

	return &Buffer{
		buckets: make(map[string]*Bucket),
		root:    o.Root,
		fs:      o.Fs,
	}
}

// Create initializes the root directory on disk.
func (b *Buffer) Create() error {
	b.Lock()
	defer b.Unlock()

	if err := b.fs.MkdirAll(b.root, 0755); err != nil {
		return err
	}

	return nil
}

// Destroy deletes the entire directory and it's contents. Use this to clean up
// when you are done using the buffer.
func (b *Buffer) Destroy() error {
	b.Lock()
	defer b.Unlock()

	if err := b.Reset(); err != nil {
		return err
	}

	if err := b.fs.RemoveAll(b.root); err != nil {
		return err
	}

	return nil
}

// Write adds the given data to named bucket. It is threadsafe and can be called
// concurrently, while maintaining the order in your buckets.
func (b *Buffer) Write(name string, data []byte) error {
	bucket, err := b.Get(name)
	if err != nil {
		return err
	}

	if err := bucket.Write(data); err != nil {
		return err
	}

	return nil
}

// Get can be used to retrieve a single bucket. If the named bucket does not
// exist, it will be created.
func (b *Buffer) Get(name string) (*Bucket, error) {
	b.Lock()
	defer b.Unlock()

	if bucket, ok := b.buckets[name]; ok {
		return bucket, nil
	}

	bucket := NewBucket(BucketOptions{
		Path: filepath.Join(b.root, name),
		Fs:   b.fs,
	})

	b.buckets[name] = bucket
	return bucket, nil
}

// Buckets retrieves the list of bucket names.
func (b *Buffer) Buckets() []string {
	b.RLock()
	defer b.RUnlock()

	list := make([]string, 0, len(b.buckets))
	for name := range b.buckets {
		list = append(list, name)
	}
	return list
}

// Flush ensures that any data held in memory is flushed to disk immediately for
// the named bucket.
func (b *Buffer) Flush(name string) error {
	bucket, err := b.Get(name)
	if err != nil {
		return err
	}

	if err := bucket.Flush(); err != nil {
		return err
	}

	return nil
}

// FlushAll ensures that all buckets have been flushed to disk immediately.
func (b *Buffer) FlushAll() error {
	for _, bucket := range b.buckets {
		if err := bucket.Flush(); err != nil {
			return err
		}
	}

	return nil
}

// Reset removes any existing buckets and restores the buffer to it's original
// clean state.
func (b *Buffer) Reset() error {
	for _, bucket := range b.buckets {
		if err := bucket.Destroy(); err != nil {
			return err
		}
	}

	// reset the internal list of buckets
	b.buckets = make(map[string]*Bucket)

	return nil
}

// Writes retrieves a full count of all writes in this buffer. This does not
// necessarily count how much has been flushed to disk.
func (b *Buffer) Writes() uint {
	b.RLock()
	defer b.RUnlock()

	var count uint
	for _, bucket := range b.buckets {
		count += bucket.Writes()
	}
	return count
}

// Bytes retrieves a full count of all bytes written to this buffer. This does
// not necessarily count how much has been flushed to disk.
func (b *Buffer) Bytes() uint64 {
	b.RLock()
	defer b.RUnlock()

	var count uint64
	for _, bucket := range b.buckets {
		count += bucket.Bytes()
	}
	return count
}

// BufferOptions is used to configure buffer instances.
type BufferOptions struct {
	// the root directory that will be used
	Root string
	// this is primarilly to allow for an in-memory filesystem during testing
	Fs afero.Fs
}

func (o *BufferOptions) defaults() {
	if o.Fs == nil {
		o.Fs = afero.NewOsFs()
	}
}
