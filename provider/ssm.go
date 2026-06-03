package provider

import (
	"context"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/ssm"
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

	if len(dest) == 0 || strings.HasSuffix(dest, "/") {
		return fmt.Errorf("Invalid file path: %s", dest)
	}

	p.mappings[src] = dest
	p.getParameterInput.Names = append(p.getParameterInput.Names, src)
	return nil
}

func (p *SsmProvider) create(path string, body string) error {
	if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
		return err
	}

	if err := os.WriteFile(path, []byte(body), 0644); err != nil {
		return err
	}
	return nil
}

func (p *SsmProvider) Output() error {
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
			go func(m string, v string) {
				defer wg.Done()
				defer func() { <-semCh }()

				if err := p.create(m, v); err != nil {
					log.Print(err)
					errCh <- err
				}
			}(p.mappings[*param.Name], *param.Value)
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
