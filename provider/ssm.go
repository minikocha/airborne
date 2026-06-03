package provider

import (
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"path"
	"path/filepath"
	"strings"
	"sync"

	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/ssm"
	"github.com/aws/aws-sdk-go-v2/service/ssm/types"
)

const (
	ssmDefaultConcurrency = 3
)

type SsmProvider struct {
	client            *ssm.Client
	concurrency       int
	mappings          map[string]string
	getParameterInput *ssm.GetParametersInput
}

type SsmProviderOption func(*SsmProvider) error

func NewSsmProvider(opts ...SsmProviderOption) *SsmProvider {
	p := &SsmProvider{
		concurrency:       ssmDefaultConcurrency,
		mappings:          make(map[string]string),
		getParameterInput: &ssm.GetParametersInput{},
	}

	if cfg, err := config.LoadDefaultConfig(context.TODO()); err != nil {
		//return err
	} else {
		p.client = ssm.NewFromConfig(cfg)
	}

	for _, f := range opts {
		if err := f(p); err != nil {
			//return err
		}
	}
	return p
}

func (p *SsmProvider) Add(src string, dest string) error {
	if len(src) == 0 || strings.HasSuffix(src, "/") {
		return fmt.Errorf("Invalid parameter name: %s", src)
	}

	p.mappings[src] = dest
	p.getParameterInput.Names = append(p.getParameterInput.Names, src)
	return nil
}

func (p *SsmProvider) create(param types.Parameter, dest string) error {
	if i, err := os.Stat(dest); err != nil && !errors.Is(err, os.ErrNotExist) {
		// NOTE: destが存在しないパス以外のエラー(e.g.: os.ErrPermission)だった場合
		return err
	} else if err == nil && i.IsDir() {
		// NOTE: destがディレクトリの場合はdest配下にファイル名を保持してコピーする
		dest = path.Join(dest, filepath.Base(*param.Name))
	} else if _, err := os.Stat(filepath.Dir(dest)); err != nil {
		// NOTE: destがディレクトリでない（ファイル扱い）状況で親ディレクトリが存在しない
		return err
	}

	if err := os.WriteFile(dest, []byte(*param.Value), 0644); err != nil {
		return err
	}
	return nil
}

func (p *SsmProvider) Output() error {
	if len(p.getParameterInput.Names) == 0 {
		return nil
	}

	output, err := p.client.GetParameters(context.TODO(), p.getParameterInput)
	if err != nil {
		return err
	}

	var wg sync.WaitGroup
	semCh := make(chan struct{}, p.concurrency)
	defer close(semCh)
	errCh := make(chan error, 1)
	defer close(errCh)
	finCh := make(chan struct{}, 1)
	defer close(finCh)

	go func() {
		for _, param := range output.Parameters {
			semCh <- struct{}{}

			wg.Add(1)
			go func(param types.Parameter, m string) {
				defer wg.Done()
				defer func() { <-semCh }()

				if err := p.create(param, m); err != nil {
					log.Print(err)
					errCh <- err
				}
			}(param, p.mappings[*param.Name])
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
