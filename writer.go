package airborne

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

type S3Writer struct {
	client *s3.Client
}

func NewS3Writer() (*S3Writer, error) {
	cfg, err := config.LoadDefaultConfig(context.TODO())
	if err != nil {
		return nil, err
	}

	return &S3Writer{client: s3.NewFromConfig(cfg)}, nil
}

func (w *S3Writer) parseArn(arn string) (bucket string, key string, err error) {
	s1 := strings.Split(arn, ":")
	if len(s1) != 6 || s1[2] != "s3" {
		return "", "", fmt.Errorf("Invalid ARN: %s", arn)
	}

	s2 := strings.SplitN(s1[5], "/", 2)
	if len(s2) != 2 {
		return "", "", fmt.Errorf("Invalid ARN: %s", arn)
	}

	return s2[0], s2[1], nil
}

func (w *S3Writer) Write(arn string, path string) error {
	bucket, key, err := w.parseArn(arn)
	if err != nil {
		return err
	}

	// ディレクトリ作成
	dir := filepath.Dir(path)
	if f, err := os.Stat(dir); err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	} else if err == nil && !f.IsDir() {
		return fmt.Errorf("File exists: %s", dir)
	}

	if err := os.MkdirAll(dir, os.ModeDir.Perm()); err != nil {
		return err
	}

	// ファイル取得
	object, err := w.client.GetObject(context.TODO(), &s3.GetObjectInput{Bucket: aws.String(bucket), Key: aws.String(key)})
	if err != nil {
		return err
	}
	defer object.Body.Close()

	body, err := io.ReadAll(object.Body)
	if err != nil {
		return nil
	}

	// ファイル作成
	fd, err := os.Create(path)
	if err != nil {
		return err
	}

	if _, err := fd.Write(body); err != nil {
		return err
	}

	return nil
}
