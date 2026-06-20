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
	defaultConcurrency = 3
)

type mapping struct {
	destinations []string
	source       *s3.GetObjectInput
}

type Handler struct {
	client      *s3.Client
	concurrency int
	mappings    map[string]*mapping
}

func New(optFns ...func(*Handler)) (*Handler, error) {
	handler := &Handler{
		concurrency: defaultConcurrency,
		mappings:    make(map[string]*mapping),
	}

	if cfg, err := config.LoadDefaultConfig(context.TODO()); err != nil {
		return nil, err
	} else {
		handler.client = s3.NewFromConfig(cfg)
	}

	for _, f := range optFns {
		f(handler)
	}

	return handler, nil
}

func (handler *Handler) Add(src string, dst string) error {
	s := strings.SplitN(src, "/", 4)
	if len(s) != 4 || s[0] != "s3:" || strings.HasSuffix(s[3], "/") {
		return fmt.Errorf("Invalid s3 path: %s", src)
	}

	if len(dst) == 0 {
		return fmt.Errorf("destination is empty")
	}

	if m, ok := handler.mappings[src]; ok {
		m.destinations = append(m.destinations, dst)
	} else {
		m = &mapping{
			destinations: []string{dst},
			source: &s3.GetObjectInput{
				Bucket: aws.String(s[2]),
				Key:    aws.String(s[3])},
		}
		handler.mappings[src] = m
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

					if err := handler.run(ctx, m); err != nil {
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

func (handler *Handler) run(ctx context.Context, mapping *mapping) error {
	output, err := handler.client.GetObject(ctx, mapping.source) // TODO: trasfermanagerの使用を検討
	if err != nil {
		return err
	}
	defer output.Body.Close()

	var w []io.Writer
	opened := make(map[string]struct{})
	for _, d := range mapping.destinations {
		i, err := os.Stat(d)
		if err != nil && !errors.Is(err, os.ErrNotExist) {
			return err
		}

		if i != nil && i.IsDir() {
			d = path.Join(d, filepath.Base(*mapping.source.Key))
		}

		d, err = filepath.Abs(d)
		if err != nil {
			return err
		}

		if _, ok := opened[d]; ok {
			continue
		}

		f, err := os.OpenFile(d, os.O_WRONLY|os.O_CREATE, 0644)
		if err != nil {
			return err
		}
		defer f.Close()

		w = append(w, f)
		opened[d] = struct{}{}
	}

	// TODO: "io.CopyBuffer()"の使用を検討
	if _, err = io.Copy(io.MultiWriter(w...), output.Body); err != nil {
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
