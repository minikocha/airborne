package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"strings"

	"github.com/minikocha/airborne"
	"github.com/minikocha/airborne/provider"
)

func main() {
	// TODO: 関係のない引数が渡された時にエラーにする
	// TODO: 既存のファイルを上書きする・しないを選択できるようにする: flag.Bool("overwrite"...)
	// TODO: S3Providerの`bufferSize`を設定できるようにする: flag.Int("buffer-size"...)
	// TODO: S3Providerの`maxConcurrency`を設定できるようにする: flag.Int("max-concurrency"...)

	versionFlag := flag.Bool("version", false, "show version")
	flag.Parse()
	if *versionFlag {
		fmt.Printf("versin: %s\n", airborne.Version())
		return
	}

	a := airborne.NewAirborne()
	a.AddProvider("s3", provider.NewS3Provider())
	a.AddProvider("ssm", provider.NewSsmProvider())

	if err := loadEnvVars(a); err != nil {
		log.Fatal(err)
	}

	if err := a.Run(); err != nil {
		log.Fatal(err)
	}
}

func loadEnvVars(a *airborne.Airborne) error {
	for _, e := range os.Environ() {
		if !strings.HasPrefix(e, "AIRBORNE_SUPPLY_") {
			continue
		}

		esd := strings.SplitN(e, "=", 3) // NOTE: npa -> Environment name, Source, Destination
		if len(esd) != 3 {
			return fmt.Errorf("Invalid environment variable: %s", e)
		}

		switch {
		case strings.HasPrefix(esd[0], "AIRBORNE_SUPPLY_S3_"):
			if err := a.AddSupply("s3", esd[1], esd[2]); err != nil {
				return err
			}
		case strings.HasPrefix(esd[0], "AIRBORNE_SUPPLY_SSM_"):
			if err := a.AddSupply("ssm", esd[1], esd[2]); err != nil {
				return err
			}
		default:
			return fmt.Errorf("No provider was found to match the given supply: %s", esd[0])
		}
	}
	return nil
}
