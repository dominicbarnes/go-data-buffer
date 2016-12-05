package buffer

import (
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

func (suite *BucketTestSuite) TestCreate() {
	suite.NoError(suite.bucket.Create())
	suite.assertFileExists(true)
}

func (suite *BucketTestSuite) TestCreateMultiple() {
	suite.NoError(suite.bucket.Create())
	suite.Error(suite.bucket.Create(), "bucket already created")
}

func (suite *BucketTestSuite) TestDestroy() {
	suite.NoError(suite.bucket.Create())
	suite.NoError(suite.bucket.Destroy())
	suite.assertFileExists(false)
}

func (suite *BucketTestSuite) TestWriteBuffered() {
	data := []byte("hello world")
	suite.NoError(suite.bucket.Write(data))
	suite.assertFileEmpty()
}

func (suite *BucketTestSuite) TestWriteFlushed() {
	data := make([]byte, 5120, 5120)
	suite.NoError(suite.bucket.Write(data))
	suite.assertFileContains(data)
}

func (suite *BucketTestSuite) TestFlush() {
	data := []byte("hello world")
	suite.NoError(suite.bucket.Write(data))
	suite.NoError(suite.bucket.Flush())
	suite.assertFileContains(data)
}

func (suite *BucketTestSuite) TestWrites() {
	data := []byte("hello world\n")
	suite.NoError(suite.bucket.Write(data))
	suite.NoError(suite.bucket.Write(data))
	suite.EqualValues(2, suite.bucket.Writes())
}

func (suite *BucketTestSuite) TestBytes() {
	data := []byte("hello world\n")
	suite.NoError(suite.bucket.Write(data))
	suite.NoError(suite.bucket.Write(data))
	suite.EqualValues(2*len(data), suite.bucket.Bytes())
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
