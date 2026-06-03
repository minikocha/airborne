package provider

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"path"
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

type S3Provider struct {
	bufferPool  sync.Pool
	bufferSize  int
	client      *s3.Client
	concurrency int
	mappings    map[string]*s3.GetObjectInput
}

type S3ProviderOption func(*S3Provider) error

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

func (p *S3Provider) Add(src string, dest string) error {
	d := strings.SplitN(src, "/", 4)
	if len(d) != 4 || d[0] != "s3:" || strings.HasSuffix(d[3], "/") {
		return fmt.Errorf("Invalid s3 path: %s", src)
	}

	p.mappings[dest] = &s3.GetObjectInput{
		Bucket: aws.String(d[2]),
		Key:    aws.String(d[3]),
	}
	return nil
}

func (p *S3Provider) copy(input *s3.GetObjectInput, dest string) error {
	if i, err := os.Stat(dest); err != nil && !errors.Is(err, os.ErrNotExist) {
		// NOTE: destが存在しないパス以外のエラー(e.g.: os.ErrPermission)だった場合
		return err
	} else if err == nil && i.IsDir() {
		// NOTE: destがディレクトリの場合はdest配下にファイル名を保持してコピーする
		dest = path.Join(dest, filepath.Base(*input.Key))
	} else if _, err := os.Stat(filepath.Dir(dest)); err != nil {
		// NOTE: destがディレクトリでない（ファイル扱い）状況で親ディレクトリが存在しない
		return err
	}

	file, err := os.Create(dest)
	if err != nil {
		return err
	}
	defer file.Close()

	output, err := p.client.GetObject(context.TODO(), input)
	if err != nil {
		return err
	}
	defer output.Body.Close()

	buf := p.bufferPool.Get().([]byte)
	defer p.bufferPool.Put(buf)
	if _, err := io.CopyBuffer(file, output.Body, buf); err != nil {
		return err
	}
	return nil
}

func (p *S3Provider) Output() error {
	if len(p.mappings) == 0 {
		return nil
	}

	var wg sync.WaitGroup
	semCh := make(chan struct{}, p.concurrency)
	defer close(semCh)
	errCh := make(chan error, 1)
	defer close(errCh)
	finCh := make(chan struct{}, 1)
	defer close(finCh)

	go func() {
		for k, v := range p.mappings {
			semCh <- struct{}{}

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
		wg.Wait()
		finCh <- struct{}{}
	}()

	select {
	case <-finCh:
		// nothing to do
	case err := <-errCh:
		return err
	}

	return nil
}
