package ssm

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
	"github.com/aws/aws-sdk-go-v2/service/ssm"
)

const (
	defaultConcurrency = 3
)

type mapping struct {
	destinations []string
	source       *ssm.GetParameterInput
}

type Handler struct {
	client      *ssm.Client
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
		handler.client = ssm.NewFromConfig(cfg)
	}

	for _, fn := range optFns {
		fn(handler)
	}

	return handler, nil
}

func (handler *Handler) Add(src string, dst string) error {
	if len(src) == 0 || strings.HasSuffix(src, "/") {
		return fmt.Errorf("Invalid parameter name: %s", src)
	}

	//TODO: destのバリデーションを実装
	if len(dst) == 0 {
		return fmt.Errorf("sss")
	}

	if m, ok := handler.mappings[src]; ok {
		m.destinations = append(m.destinations, dst)
	} else {
		m = &mapping{
			destinations: []string{dst},
			source:       &ssm.GetParameterInput{Name: aws.String(src)},
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

					// TODO: mappingそのものを渡すよう変更
					if err := handler.run(ctx, m.source, m.destinations); err != nil {
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

func (handler *Handler) run(ctx context.Context, src *ssm.GetParameterInput, dsts []string) error {
	output, err := handler.client.GetParameter(ctx, src)
	if err != nil {
		return err
	}

	var w []io.Writer
	opened := make(map[string]struct{})
	for _, d := range dsts {
		i, err := os.Stat(d)
		if err != nil && !errors.Is(err, os.ErrNotExist) {
			return err
		}

		if i != nil && i.IsDir() {
			d = path.Join(d, filepath.Base(*src.Name))
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

	if _, err := io.Copy(io.MultiWriter(w...), strings.NewReader(*output.Parameter.Value)); err != nil {
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
	return "ssm"
}
