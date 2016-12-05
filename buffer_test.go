package buffer

import (
	"testing"

	"github.com/spf13/afero"
	"github.com/stretchr/testify/suite"
)

type BufferTestSuite struct {
	suite.Suite
	buffer *Buffer
}

func TestBufferTestSuite(t *testing.T) {
	suite.Run(t, new(BufferTestSuite))
}

func (suite *BufferTestSuite) SetupTest() {
	suite.buffer = NewBuffer(BufferOptions{
		Root: "./test",
		Fs:   afero.NewMemMapFs(),
	})
}

func (suite *BufferTestSuite) TestCreate() {
	suite.NoError(suite.buffer.Create())
	suite.assertBufferRootExists(true)
}

func (suite *BufferTestSuite) TestDestroy() {
	suite.NoError(suite.buffer.Create())
	suite.NoError(suite.buffer.Destroy())
	suite.assertBufferRootExists(false)
}

func (suite *BufferTestSuite) TestGetNewBucket() {
	bucket, err := suite.buffer.Get("new")
	suite.NoError(err)
	suite.NotNil(bucket)
}

func (suite *BufferTestSuite) TestGetExistingBucket() {
	bucket1, err := suite.buffer.Get("existing")
	bucket2, err := suite.buffer.Get("existing")
	suite.NoError(err)
	suite.Equal(bucket1, bucket2)
}

func (suite *BufferTestSuite) TestWrite() {
	data := []byte("hello world")
	suite.NoError(suite.buffer.Write("1", data))
	bucket, err := suite.buffer.Get("1")
	suite.NoError(err)
	suite.EqualValues(1, bucket.Writes())
	suite.EqualValues(len(data), bucket.Bytes())
}

func (suite *BufferTestSuite) TestFlush() {
	data := []byte("hello world\n")
	suite.NoError(suite.buffer.Write("1", data))
	suite.assertBucketFileEmpty("1")
	suite.NoError(suite.buffer.Flush("1"))
	suite.assertBucketFileContains("1", data)
}

func (suite *BufferTestSuite) TestFlushAll() {
	data := []byte("hello world\n")
	suite.NoError(suite.buffer.Write("1", data))
	suite.NoError(suite.buffer.Write("2", data))
	suite.NoError(suite.buffer.FlushAll())
	suite.assertBucketFileContains("1", data)
	suite.assertBucketFileContains("2", data)
}

func (suite *BufferTestSuite) TestBuckets() {
	data := []byte("hello world\n")
	suite.NoError(suite.buffer.Write("1", data))
	suite.NoError(suite.buffer.Write("2", data))
	actual := suite.buffer.Buckets()
	suite.Len(actual, 2)
	suite.Contains(actual, "1")
	suite.Contains(actual, "2")
}

func (suite *BufferTestSuite) TestReset() {
	data := []byte("hello world\n")
	suite.NoError(suite.buffer.Write("1", data))
	suite.NoError(suite.buffer.Write("2", data))
	suite.NoError(suite.buffer.Reset())
	suite.Empty(suite.buffer.Buckets())
}

func (suite *BufferTestSuite) TestWrites() {
	data := []byte("hello world\n")
	suite.NoError(suite.buffer.Write("1", data))
	suite.NoError(suite.buffer.Write("2", data))
	suite.EqualValues(2, suite.buffer.Writes())
}

func (suite *BufferTestSuite) TestBytes() {
	data := []byte("hello world\n")
	suite.NoError(suite.buffer.Write("1", data))
	suite.NoError(suite.buffer.Write("2", data))
	suite.EqualValues(2*len(data), suite.buffer.Bytes())
}

func (suite *BufferTestSuite) assertBufferRootExists(expected bool) {
	actual, err := afero.Exists(suite.buffer.fs, suite.buffer.root)
	suite.NoError(err)
	suite.Equal(expected, actual)
}

func (suite *BufferTestSuite) assertBucketFileExists(name string, expected bool) {
	bucket, err := suite.buffer.Get(name)
	suite.NoError(err)
	actual, err := afero.Exists(bucket.fs, bucket.path)
	suite.NoError(err)
	suite.Equal(expected, actual)
}

func (suite *BufferTestSuite) assertBucketFileEmpty(name string) {
	bucket, err := suite.buffer.Get(name)
	suite.NoError(err)
	empty, err := afero.IsEmpty(bucket.fs, bucket.path)
	suite.NoError(err)
	suite.True(empty)
}

func (suite *BufferTestSuite) assertBucketFileContains(name string, data []byte) {
	bucket, err := suite.buffer.Get(name)
	suite.NoError(err)
	contains, err := afero.FileContainsBytes(bucket.fs, bucket.path, data)
	suite.NoError(err)
	suite.True(contains)
}
