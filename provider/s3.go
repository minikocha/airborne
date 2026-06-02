package provider

import (
	"context"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

const (
	s3DefaultBufferSize  = 32 // Set initial buffer size to (1024byte * defaultBufferSize)
	s3DefaultConcurrency = 3
)

type S3ProviderOption func(*S3Provider) error

type S3Provider struct {
	bufferPool  sync.Pool
	bufferSize  int
	client      *s3.Client
	concurrency int
	mappings    map[string]*s3.GetObjectInput
}

func NewS3Provider(opts ...S3ProviderOption) *S3Provider {
	p := &S3Provider{
		bufferSize:  s3DefaultBufferSize,
		concurrency: s3DefaultConcurrency,
		mappings:    make(map[string]*s3.GetObjectInput),
	}

	if cfg, err := config.LoadDefaultConfig(context.TODO()); err != nil {
		//return err
	} else {
		p.client = s3.NewFromConfig(cfg)
	}

	for _, f := range opts {
		if err := f(p); err != nil {
			//return err
		}
	}

	p.bufferPool = sync.Pool{New: func() any { return make([]byte, 1024*p.bufferSize) }}
	return p
}

//func WithBufferSize(size int) S3ProviderOption {
//	return func(p *S3Provider) error {
//		if size < 1 {
//			return fmt.Errorf("buffer size must be greater than 0: %d", size)
//		}
//
//		p.bufferSize = size
//		return nil
//	}
//}

//func WithMaxConcurrency(num int) S3ProviderOption {
//	return func(p *S3Provider) error {
//		if num < 1 {
//			return fmt.Errorf("max concurrency must be greater than 0: %d", num)
//		}
//
//		p.concurrency = num
//		return nil
//	}
//}

func (p *S3Provider) Add(src string, dest string) error {
	d := strings.SplitN(src, "/", 4)
	if len(d) != 4 || d[0] != "s3:" || strings.HasSuffix(d[3], "/") {
		return fmt.Errorf("Invalid s3 path: %s", src)
	}

	if len(dest) == 0 || strings.HasSuffix(dest, "/") {
		return fmt.Errorf("Invalid file path: %s", dest)
	}

	p.mappings[dest] = &s3.GetObjectInput{
		Bucket: aws.String(d[2]),
		Key:    aws.String(d[3]),
	}
	return nil
}

func (p *S3Provider) copy(input *s3.GetObjectInput, path string) error {
	output, err := p.client.GetObject(context.TODO(), input)
	if err != nil {
		return err
	}
	defer output.Body.Close()

	if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
		return err
	}

	file, err := os.Create(path)
	if err != nil {
		return err
	}
	defer file.Close()

	buf := p.bufferPool.Get().([]byte)
	defer p.bufferPool.Put(buf)
	if _, err := io.CopyBuffer(file, output.Body, buf); err != nil {
		return err
	}
	return nil
}

func (p *S3Provider) Output() error {
	var wg sync.WaitGroup
	semCh := make(chan struct{}, p.concurrency)
	defer close(semCh)
	errCh := make(chan error, 1)
	//defer close(err)

	for k, v := range p.mappings {
		log.Printf("s3://%s/%s -> %s", *v.Bucket, *v.Key, k)

		select {
		case err := <-errCh:
			log.Print("error returned")
			wg.Wait()
			return err
		case semCh <- struct{}{}:
			wg.Add(1)
			go func(k string, v *s3.GetObjectInput) {
				defer wg.Done()
				defer func() { <-semCh }()

				if err := p.copy(v, k); err != nil {
					log.Print(err)
					errCh <- err
				}
			}(k, v)
		}
	}
	wg.Wait() // TODO: forを抜けた後のゴルーチンのエラーをケアできていない
	return nil
}
