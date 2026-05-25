package airborne

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/ssm"
)

const (
	DEFAULT_BUFFER_SIZE     = 32 // buffer size for `io.CopyBuffer()` (1024 byte * DEFAULT_BUFFER_SIZE)
	DEFAULT_MAX_CONCURRENCY = 2  // maximum number of concurrent processings
)

type MappingFunc func() error

type Airborne struct {
	bufferSize     int
	maxConcurrency int
	bufferPool     sync.Pool

	s3Client  *s3.Client
	ssmClient *ssm.Client

	mappings []MappingFunc
}

type Option func(*Airborne) error

func WithBufferSize(size int) Option {
	return func(a *Airborne) error {
		if size < 1 {
			return fmt.Errorf("buffer size must be greater than 0: %d", size)
		}

		a.bufferSize = size
		return nil
	}
}

func WithMaxConcurrency(num int) Option {
	return func(a *Airborne) error {
		if num < 1 {
			return fmt.Errorf("max concurrency must be greater than 0: %d", num)
		}

		a.maxConcurrency = num
		return nil
	}
}

func NewAirborne(opts ...Option) (*Airborne, error) {
	todo := context.TODO()
	cfg, err := config.LoadDefaultConfig(todo)
	if err != nil {
		return nil, err
	}
	s3Client := s3.NewFromConfig(cfg)
	ssmClient := ssm.NewFromConfig(cfg)

	a := &Airborne{
		bufferSize:     DEFAULT_BUFFER_SIZE,
		maxConcurrency: DEFAULT_MAX_CONCURRENCY,

		s3Client:  s3Client,
		ssmClient: ssmClient,
	}

	for _, f := range opts {
		if err := f(a); err != nil {
			return nil, err
		}
	}

	a.bufferPool = sync.Pool{New: func() any { return make([]byte, 1024*a.bufferSize) }}
	return a, nil
}

// TODO: `func(a *Airborne) LoadEnvVars() error {}`を実装する

func (a *Airborne) AddS3Mapping(m *S3Mapping) {
	a.mappings = append(
		a.mappings,
		func() error {
			output, err := a.s3Client.GetObject(context.TODO(), &s3.GetObjectInput{Bucket: aws.String(m.Bucket), Key: aws.String(m.Key)})
			if err != nil {
				return err
			}
			defer output.Body.Close()

			// TODO: ディレクトリ作成処理を外だしする（ここから）
			dir := filepath.Dir(m.Path)
			if f, err := os.Stat(dir); err != nil && !errors.Is(err, os.ErrNotExist) {
				return err
			} else if err == nil && !f.IsDir() {
				return fmt.Errorf("File exists: %s", dir)
			}

			if err := os.MkdirAll(dir, os.ModeDir.Perm()); err != nil {
				return err
			}
			// TODO: ディレクトリ作成処理を外だしする（ここまで）

			file, err := os.Create(m.Path)
			if err != nil {
				return err
			}
			defer file.Close()

			buf := a.bufferPool.Get().([]byte)
			defer a.bufferPool.Put(buf)
			if _, err := io.CopyBuffer(file, output.Body, buf); err != nil {
				return err
			}
			return nil
		},
	)
}

// TODO
//
//	実装を見直す必要あり？
//	都度`GetParameter()`を実行するより、まとめて`GetParameters()`した方が早そう
func (a *Airborne) AddSSMMapping(m *SSMMapping) {
	a.mappings = append(
		a.mappings,
		func() error {
			output, err := a.ssmClient.GetParameter(context.TODO(), &ssm.GetParameterInput{Name: aws.String(m.Parameter), WithDecryption: aws.Bool(true)})
			if err != nil {
				return err
			}

			// TODO: ディレクトリ作成処理を外だしする（ここから）
			dir := filepath.Dir(m.Path)
			if f, err := os.Stat(dir); err != nil && !errors.Is(err, os.ErrNotExist) {
				return err
			} else if err == nil && !f.IsDir() {
				return fmt.Errorf("File exists: %s", dir)
			}

			if err := os.MkdirAll(dir, os.ModeDir.Perm()); err != nil {
				return err
			}
			// TODO: ディレクトリ作成処理を外だしする（ここまで）

			file, err := os.Create(m.Path)
			if err != nil {
				return err
			}
			defer file.Close()

			if _, err := file.Write([]byte(*output.Parameter.Value)); err != nil {
				return err
			}
			return nil
		},
	)
}

func (a *Airborne) Run() error {
	// TODO: `context.Background()`と`signal.NotifyContext()`を使って処理の中断を実装する

	var wg sync.WaitGroup
	semaphore := make(chan struct{}, a.maxConcurrency)
	defer close(semaphore)
	errCh := make(chan error, 1)

	for _, f := range a.mappings {
		semaphore <- struct{}{} // 空き枠が出るまでブロック
		select {
		case err := <-errCh:
			// TODO: ここでreturnしたときに実行中だったGoルーチンのケアを実装する
			//       1. 実行中のGoルーチンは終了を待機する。(`wg.Wait()`？)
			//       2. 残りの処理は実行しない。(ここで`wg.Wait()`していればブロックされて、`default`ブロックは実行されないはず)
			return err
		default:
			wg.Add(1)
			go func(f func() error) {
				defer wg.Done()
				defer func() { <-semaphore }() // 終わったら枠を空ける

				if err := f(); err != nil {
					errCh <- err
				}
			}(f)
		}
	}
	wg.Wait() // すべてのGoルーチンが終了するのを待つ
	close(errCh)
	return nil
}

// ---
type S3Mapping struct {
	Bucket string
	Key    string
	Path   string
}

type SSMMapping struct {
	Parameter string // nameで良いかも(`aws ssm get-parameter()`と同じで)
	Region    string // TODO: 実行環境と異なるリージョンが指定された場合どうする？
	Path      string
}

//type Writer interface {
//	Write(string, string) error
//}
//
//type Supply struct {
//	arn    string
//	path   string
//	writer Writer
//}
//
//type Airborne struct {
//	supplies []*Supply
//}
//
//func NewAirborne() *Airborne {
//	return &Airborne{}
//}
//
//func (a *Airborne) Add(s *Supply) {
//	a.supplies = append(a.supplies, s)
//}
//
//func (a *Airborne) LoadEnvVars() error {
//	for _, e := range os.Environ() {
//		// NOTE: 環境変数は`AIRBORNE_SUPPLY_<ITEM>_<SUFFIX>="<ARN>=<PATH>"`で渡されることを想定
//		//       e.g.: `AIRBORNE_SUPPLY_S3_1="/path/to/file=arn:aws:s3:::example-bucket/some-object"`
//		if !strings.HasPrefix(e, "AIRBORNE_SUPPLY_") {
//			continue
//		}
//
//		nap := strings.SplitN(e, "=", 3) // NOTE: npa: Name, Arn, Path
//		if len(nap) != 3 {
//			return fmt.Errorf("Invalid Value: %s", e)
//		}
//
//		switch {
//		// TODO: SECRETの実装
//		// TODO: SSM_BY_PATHの実装
//		// TODO: SSMの実装
//		// TODO: S3_SYNCの実装
//		case strings.HasPrefix(nap[0], "AIRBORNE_SUPPLY_S3_"):
//			w, err := NewS3Writer()
//			if err != nil {
//				return err
//			}
//			a.Add(&Supply{arn: nap[1], path: nap[2], writer: w})
//		default:
//			return fmt.Errorf("Unknown item: %s", nap[0])
//		}
//	}
//
//	return nil
//}
//
//func (a *Airborne) WriteAll() error {
//	// TODO: goroutineで並行処理を実装する
//	for _, s := range a.supplies {
//		if err := s.writer.Write(s.arn, s.path); err != nil {
//			return err
//		}
//	}
//	return nil
//}
