package provider

import (
	"sync"

	"github.com/aws/aws-sdk-go-v2/service/s3"
)

const (
	s3syncDefaultBufferSize  = 32 // Set initial buffer size to (1024byte * defaultBufferSize)
	s3syncDefaultConcurrency = 3
)

type S3syncProvider struct {
	bufferPool  sync.Pool
	bufferSize  int
	client      *s3.Client
	concurrency int
	//mappings    map[string]*s3.GetObjectInput
}
