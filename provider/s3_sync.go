package provider

import (
	"context"
	"fmt"
	"strings"
	"sync"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/feature/s3/transfermanager"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

const (
	s3SyncDefaultConcurrency = 3
)

type S3SyncProvider struct {
	client                  *transfermanager.Client
	concurrency             int
	downloadDirectoryInputs []*transfermanager.DownloadDirectoryInput
}

type S3SyncProviderOption func(*S3SyncProvider) error

func NewS3SyncProvider(opts ...S3SyncProviderOption) *S3SyncProvider {
	p := &S3SyncProvider{concurrency: s3SyncDefaultConcurrency}
	if cfg, err := config.LoadDefaultConfig(context.TODO()); err != nil {
		//return err
	} else {
		c := s3.NewFromConfig(cfg)
		p.client = transfermanager.New(c)
	}

	for _, f := range opts {
		if err := f(p); err != nil {
			//return err
		}
	}
	return p
}

func (p *S3SyncProvider) Add(src string, dest string) error {
	s := strings.SplitN(src, "/", 4)
	if len(s) != 4 || s[0] != "s3:" {
		return fmt.Errorf("Invalid s3 path: %s", src)
	}

	p.downloadDirectoryInputs = append(
		p.downloadDirectoryInputs,
		&transfermanager.DownloadDirectoryInput{
			Bucket:        aws.String(s[2]),
			Destination:   aws.String(dest),
			KeyPrefix:     aws.String(s[3]),
			FailurePolicy: &transfermanager.TerminateDownloadPolicy{},
		},
	)
	return nil
}

func (p *S3SyncProvider) Output() error {
	if len(p.downloadDirectoryInputs) == 0 {
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
		for _, v := range p.downloadDirectoryInputs {
			semCh <- struct{}{}

			wg.Add(1)
			go func(v *transfermanager.DownloadDirectoryInput) {
				defer wg.Done()
				defer func() { <-semCh }()

				if _, err := p.client.DownloadDirectory(context.TODO(), v); err != nil {
					errCh <- err
				}
			}(v)
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
