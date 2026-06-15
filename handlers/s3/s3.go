package s3

import (
	"context"
	"errors"
	"fmt"
	"io"
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
	defaultBufferSize  = 32 // Set initial buffer size to (1024byte * defaultBufferSize)
	defaultConcurrency = 3
)

type mapping struct {
	dest string
	src  *s3.GetObjectInput
}

type Handler struct {
	bufferPool  sync.Pool
	bufferSize  int
	client      *s3.Client
	concurrency int
	mappings    []*mapping
}

type Option func(*Handler) error

//func WithBufferSize(i int) Option {
//	return func(handler *Handler) error {
//		if i < 1 {
//			return fmt.Errorf("buffer size must be greater than 0: %d", i)
//		}
//
//		handler.bufferSize = i
//		return nil
//	}
//}

//func WithMaxConcurrency(i int) Option {
//	return func(handler *Handler) error {
//		if i < 1 {
//			return fmt.Errorf("max concurrency must be greater than 0: %d", i)
//		}
//
//		handler.concurrency = i
//		return nil
//	}
//}

func NewHandler(opts ...Option) (*Handler, error) {
	handler := &Handler{
		bufferSize:  defaultBufferSize,
		concurrency: defaultConcurrency,
	}

	if cfg, err := config.LoadDefaultConfig(context.TODO()); err != nil {
		return nil, err
	} else {
		handler.client = s3.NewFromConfig(cfg)
	}

	for _, f := range opts {
		if err := f(handler); err != nil {
			return nil, err
		}
	}

	handler.bufferPool = sync.Pool{New: func() any { return make([]byte, 1024*handler.bufferSize) }}

	return handler, nil
}

func (handler *Handler) Add(src string, dest string) error {
	s := strings.SplitN(src, "/", 4)
	if len(s) != 4 || s[0] != "s3:" || strings.HasSuffix(s[3], "/") {
		return fmt.Errorf("Invalid s3 path: %s", src)
	}

	handler.mappings = append(
		handler.mappings,
		&mapping{
			dest: dest,
			src: &s3.GetObjectInput{
				Bucket: aws.String(s[2]),
				Key:    aws.String(s[3])}},
	)
	return nil
}

func (handler *Handler) Run(ctx context.Context) error {
	if len(handler.mappings) == 0 {
		return nil
	}

	errCh := make(chan error, 1)
	go func() {
		defer close(errCh)

		var wg sync.WaitGroup
		semCh := make(chan struct{}, handler.concurrency)
		defer close(semCh)

		for _, m := range handler.mappings {
			select {
			case <-ctx.Done():
				return
			case semCh <- struct{}{}:
				wg.Go(func() {
					defer func() { <-semCh }()

					if err := handler.run(ctx, m.src, m.dest); err != nil {
						// NOTE: to prevent panic: sending to a closed channel
						defer func() {
							if r := recover(); r != nil {
								return
							}
						}()
						errCh <- err
					}
				})
			}
		}
		wg.Wait()
	}()

	select {
	case err := <-errCh:
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (handler *Handler) run(ctx context.Context, input *s3.GetObjectInput, dest string) error {
	if i, err := os.Stat(dest); err != nil && !errors.Is(err, os.ErrNotExist) {
		// NOTE: when an error other than os.ErrNotExist occurs
		return err
	} else if err == nil && i.IsDir() {
		// NOTE: when `dest` is a directory
		dest = path.Join(dest, filepath.Base(*input.Key))
	} else if _, err := os.Stat(filepath.Dir(dest)); err != nil {
		// NOTE: when `dest`'s parent directory does not exist
		return err
	}

	file, err := os.Create(dest)
	if err != nil {
		return err
	}
	defer file.Close()

	output, err := handler.client.GetObject(ctx, input)
	if err != nil {
		return err
	}
	defer output.Body.Close()

	buf := handler.bufferPool.Get().([]byte)
	defer handler.bufferPool.Put(buf)
	if _, err := io.CopyBuffer(file, output.Body, buf); err != nil {
		return err
	}

	return nil
}

func (handler *Handler) SetConcurrency(i int) error {
	if i < 1 {
		return fmt.Errorf("%s: Concurrency must be set to a value greater than 0: %d", handler.Type(), i)
	}

	handler.concurrency = i
	return nil
}

func (handler *Handler) Type() string {
	return "s3"
}
