package s3

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
	defaultBufferSize  = 32 // Set initial buffer size to (1024byte * defaultBufferSize)
	defaultConcurrency = 3
)

type mapping struct {
	dest  map[string]struct{}
	input *s3.GetObjectInput
}

func NewMapping(dest string, input *s3.GetObjectInput) *mapping {
	m := &mapping{dest: make(map[string]struct{})}
	m.dest[dest] = struct{}{}
	m.input = input
	return m
}

type Handler struct {
	bufferPool  sync.Pool
	bufferSize  int
	client      *s3.Client
	concurrency int
	mappings    map[string]*mapping
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
		mappings:    make(map[string]*mapping),
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

	dest = strings.TrimSuffix(dest, "/")

	if m, ok := handler.mappings[src]; ok {
		m.dest[dest] = struct{}{}
	} else {
		handler.mappings[src] = NewMapping(
			dest,
			&s3.GetObjectInput{
				Bucket: aws.String(s[2]),
				Key:    aws.String(s[3])},
		)
	}

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

					if err := handler.run(ctx, m.input, m.dest); err != nil {
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

func (handler *Handler) run(ctx context.Context, input *s3.GetObjectInput, dest map[string]struct{}) error {
	output, err := handler.client.GetObject(ctx, input)
	if err != nil {
		return err
	}
	defer output.Body.Close()
	log.Printf("download s3://%s/%s\n", *input.Bucket, *input.Key) // debug

	tmp, err := os.CreateTemp("", "")
	if err != nil {
		return err
	}
	defer func() {
		tmp.Close()
		os.Remove(tmp.Name())
	}()

	buf := handler.bufferPool.Get().([]byte)
	defer handler.bufferPool.Put(buf)
	if _, err = io.CopyBuffer(tmp, output.Body, buf); err != nil {
		return err
	}

	if _, err = tmp.Seek(0, io.SeekStart); err != nil {
		return err
	}

	var w []io.Writer
	for d := range dest {
		if i, err := os.Stat(d); err != nil && !errors.Is(err, os.ErrNotExist) {
			// NOTE: when an error other than os.ErrNotExist occurs
			return err
		} else if err == nil && i.IsDir() {
			// NOTE: when `dest` is a directory
			d = path.Join(d, filepath.Base(*input.Key))
		} else if _, err = os.Stat(filepath.Dir(d)); err != nil {
			// NOTE: when `dest`'s parent directory does not exist
			return err
		}

		f, err := os.OpenFile(d, os.O_WRONLY|os.O_CREATE, 0644)
		if err != nil {
			return err
		}

		w = append(w, f)
		defer f.Close()
		log.Printf("copy s3://%s/%s to %s\n", *input.Bucket, *input.Key, d) // debug
	}

	if _, err = io.CopyBuffer(io.MultiWriter(w...), tmp, buf); err != nil {
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
