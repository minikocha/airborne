package airborne

import (
	"fmt"
	"os"
	"strings"
)

type Writer interface {
	Write(string, string) error
}

type Supply struct {
	arn    string
	path   string
	writer Writer
}

type Airborne struct {
	supplies []*Supply
}

func NewAirborne() *Airborne {
	return &Airborne{}
}

func (a *Airborne) Add(s *Supply) {
	a.supplies = append(a.supplies, s)
}

func (a *Airborne) LoadEnvVars() error {
	for _, e := range os.Environ() {
		// NOTE: 環境変数は`AIRBORNE_SUPPLY_<ITEM>_<SUFFIX>="<ARN>=<PATH>"`で渡されることを想定
		//       e.g.: `AIRBORNE_SUPPLY_S3_1="/path/to/file=arn:aws:s3:::example-bucket/some-object"`
		if !strings.HasPrefix(e, "AIRBORNE_SUPPLY_") {
			continue
		}

		nap := strings.SplitN(e, "=", 3) // NOTE: npa: Name, Arn, Path
		if len(nap) != 3 {
			return fmt.Errorf("Invalid Value: %s", e)
		}

		switch {
		// TODO: SECRETの実装
		// TODO: SSM_BY_PATHの実装
		// TODO: SSMの実装
		// TODO: S3_SYNCの実装
		case strings.HasPrefix(nap[0], "AIRBORNE_SUPPLY_S3_"):
			w, err := NewS3Writer()
			if err != nil {
				return err
			}
			a.Add(&Supply{arn: nap[1], path: nap[2], writer: w})
		default:
			return fmt.Errorf("Unknown item: %s", nap[0])
		}
	}

	return nil
}

func (a *Airborne) WriteAll() error {
	// TODO: goroutineで並行処理を実装する
	for _, s := range a.supplies {
		if err := s.writer.Write(s.arn, s.path); err != nil {
			return err
		}
	}
	return nil
}
