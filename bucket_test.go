package buffer

import (
	"io"
	"io/ioutil"
	"testing"

	"github.com/spf13/afero"
	"github.com/stretchr/testify/suite"
)

type BucketTestSuite struct {
	suite.Suite
	bucket *Bucket
}

func TestBucketTestSuite(t *testing.T) {
	suite.Run(t, new(BucketTestSuite))
}

func (suite *BucketTestSuite) SetupTest() {
	suite.bucket = NewBucket(BucketOptions{
		Path: "./test/a",
		Fs:   afero.NewMemMapFs(),
	})
}

func (suite *BucketTestSuite) TestOpen() {
	suite.NoError(suite.bucket.Open())
	suite.assertFileExists(true)
}

func (suite *BucketTestSuite) TestOpenMultiple() {
	suite.NoError(suite.bucket.Open())
	suite.Error(suite.bucket.Open(), "bucket already open")
}

func (suite *BucketTestSuite) TestClose() {
	suite.NoError(suite.bucket.Open())
	data := []byte("hello world")
	suite.NoError(suite.bucket.Write(data))
	suite.NoError(suite.bucket.Close())
}

func (suite *BucketTestSuite) TestCloseSeek() {
	suite.NoError(suite.bucket.Open())
	data := []byte("hello world")
	suite.NoError(suite.bucket.Write(data))
	suite.NoError(suite.bucket.Close())
	pos, err := suite.bucket.file.Seek(0, io.SeekCurrent)
	suite.NoError(err)
	suite.EqualValues(pos, 0)
}

func (suite *BucketTestSuite) TestDestroy() {
	suite.NoError(suite.bucket.Open())
	suite.NoError(suite.bucket.Destroy())
	suite.assertFileExists(false)
}

func (suite *BucketTestSuite) TestWriteUnopened() {
	data := []byte("hello world")
	suite.Error(suite.bucket.Write(data), "bucket not accepting writes, make sure to open it first")
}

func (suite *BucketTestSuite) TestWriteFlushed() {
	suite.NoError(suite.bucket.Open())
	data := make([]byte, 5120, 5120)
	suite.NoError(suite.bucket.Write(data))
	suite.assertFileContains(data)
}

func (suite *BucketTestSuite) TestWrites() {
	suite.NoError(suite.bucket.Open())
	data := []byte("hello world\n")
	suite.NoError(suite.bucket.Write(data))
	suite.NoError(suite.bucket.Write(data))
	suite.EqualValues(2, suite.bucket.Writes())
}

func (suite *BucketTestSuite) TestBytes() {
	suite.NoError(suite.bucket.Open())
	data := []byte("hello world\n")
	suite.NoError(suite.bucket.Write(data))
	suite.NoError(suite.bucket.Write(data))
	suite.EqualValues(2*len(data), suite.bucket.Bytes())
}

func (suite *BucketTestSuite) TestReader() {
	suite.Implements((*io.Reader)(nil), suite.bucket)
}

func (suite *BucketTestSuite) TestRead() {
	suite.NoError(suite.bucket.Open())
	data := []byte("hello world\n")
	suite.NoError(suite.bucket.Write(data))
	suite.NoError(suite.bucket.Close())
	actual, err := ioutil.ReadAll(suite.bucket)
	suite.NoError(err)
	suite.EqualValues(data, actual)
}

func (suite *BucketTestSuite) TestReadStillOpen() {
	suite.NoError(suite.bucket.Open())
	_, err := ioutil.ReadAll(suite.bucket)
	suite.Error(err, "bucket accepting writes, make sure to close before reading")
}

func (suite *BucketTestSuite) assertFileExists(expected bool) {
	actual, err := afero.Exists(suite.bucket.fs, suite.bucket.path)
	suite.NoError(err)
	suite.Equal(expected, actual)
}

func (suite *BucketTestSuite) assertFileEmpty() {
	empty, err := afero.IsEmpty(suite.bucket.fs, suite.bucket.path)
	suite.NoError(err)
	suite.True(empty)
}

func (suite *BucketTestSuite) assertFileContains(data []byte) {
	contains, err := afero.FileContainsBytes(suite.bucket.fs, suite.bucket.path, data)
	suite.NoError(err)
	suite.True(contains)
}
