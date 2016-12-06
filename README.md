# go-data-buffer

[![godoc][godoc-badge]][godoc]

> A data buffer system that uses the disk as an intermediate store between
> different stages in a data transformation (eg: ETL) process.

This module takes inspiration from [tj/go-disk-buffer][tj] and
[segmentio/go-disk-buffer][segment] but intentionally remains unmanaged,
allowing your application to determine how that data is managed on disk and in
your workflow.

Each "buffer" is a collection of "buckets". You can use this to consume data and
partition it before moving onto the next phase of processing. If you just want
to buffer data 1:1, you can instantiate the `Bucket` directly, you'll just need
to assign your own filename.

## Example

```go
import (
  "ioutil"
  "log"

  "github.com/dominicbarnes/go-data-buffer"
)

func main() {
  buffer := buffer.NewBuffer(buffer.BufferOptions{Root: "./data"})

  // create the necessary directory on disk
  if err := buffer.Open(); err != nil {
    log.Fatal(err)
  }

  // this module is thread safe and can be called from many goroutines
  if err := buffer.Write("bucket", []byte("hello world")); err != nil {
    log.Fatal(err)
  }

  // when you are ready to start consuming from these files, close the buffer
  if err := buffer.Close(); err != nil {
    log.Fatal(err)
  }

  if bucket, err := buffer.Get("bucket"); err != nil {
    log.Fatal(err)
  } else {
    if data, err := ioutil.ReadAll(bucket); err != nil {
      log.Fatal(err)
    } else {
      log.Print(data)
    }
  }

  // you can clean up after you're done or no longer need the stuff on disk
  if err := buffer.Destroy(); err != nil {
    log.Fatal(err)
  }
}
```


[godoc-badge]: https://godoc.org/github.com/dominicbarnes/go-data-buffer?status.svg
[godoc]: https://godoc.org/github.com/dominicbarnes/go-data-buffer
[segment]: https://github.com/segmentio/go-disk-buffer
[tj]: https://github.com/tj/go-disk-buffer
