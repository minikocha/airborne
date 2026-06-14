package provider

import (
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"path"
	"path/filepath"
	"sync"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/ssm"
)

const (
	ssmByPathDefaultBatchSize   = 10
	ssmByPathDefaultConcurrency = 3
)

type SsmByPathProvider struct {
	batchSize   int32
	client      *ssm.Client
	concurrency int
	mappings    map[string]string
	inputs      []*ssm.GetParametersByPathInput
}

type SsmByPathProviderOption func(*SsmByPathProvider) error

func NewSsmByPathProvider(opts ...SsmByPathProviderOption) *SsmByPathProvider {
	p := &SsmByPathProvider{
		batchSize:   ssmByPathDefaultBatchSize,
		concurrency: ssmByPathDefaultConcurrency,
		mappings:    make(map[string]string),
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

func (p *SsmByPathProvider) Add(src string, dest string) error {
	if len(src) == 0 {
		return fmt.Errorf("Invalid parameter path: %s", src)
	}

	p.mappings[src] = dest
	p.inputs = append(
		p.inputs,
		&ssm.GetParametersByPathInput{
			Path:           aws.String(src),
			MaxResults:     aws.Int32(p.batchSize),
			NextToken:      nil,
			Recursive:      aws.Bool(false),
			WithDecryption: aws.Bool(true),
		},
	)
	return nil
}

func (p *SsmByPathProvider) create(input *ssm.GetParametersByPathInput, dest string) error {
	if i, err := os.Stat(dest); err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	} else if errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("no such directory: %s", dest)
	} else if !i.IsDir() {
		return fmt.Errorf("File already exists: %s", dest)
	}

	for {
		output, err := p.client.GetParametersByPath(context.TODO(), input)
		if err != nil {
			return err
		}

		for _, p := range output.Parameters {
			if err := os.WriteFile(path.Join(dest, filepath.Base(*p.Name)), []byte([]byte(*p.Value)), 0644); err != nil {
				return err
			}
		}

		if output.NextToken == nil {
			break
		}
		input.NextToken = output.NextToken
	}
	return nil
}

func (p *SsmByPathProvider) Output() error {
	if len(p.inputs) == 0 {
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
		for _, i := range p.inputs {
			semCh <- struct{}{}

			wg.Add(1)
			go func(input *ssm.GetParametersByPathInput, s string) {
				defer wg.Done()
				defer func() { <-semCh }()

				if err := p.create(input, s); err != nil {
					log.Print(err)
					errCh <- err
				}
			}(i, p.mappings[*i.Path])
		}
		wg.Wait()
		finCh <- struct{}{}
	}()

	select {
	case <-finCh:
	case err := <-errCh:
		return err
	}
	return nil
}
