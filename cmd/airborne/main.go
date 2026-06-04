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
	versionFlag := flag.Bool("version", false, "show version")
	flag.Parse()
	if *versionFlag {
		fmt.Printf("versin: %s\n", airborne.Version())
		return
	}

	a := airborne.NewAirborne()
	a.AddProvider("ssm", provider.NewSsmProvider())
	a.AddProvider("s3sync", provider.NewS3SyncProvider())
	a.AddProvider("s3", provider.NewS3Provider())

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

		kv := strings.SplitN(e, "=", 2)
		if len(kv) != 2 {
			return fmt.Errorf("Invalid environment variable: %s", e)
		}

		pos := strings.SplitN(kv[1], "|", 2)
		if len(pos) != 2 {
			return fmt.Errorf("Invalid environment variable: %s", e)
		}

		switch {
		case strings.HasPrefix(kv[0], "AIRBORNE_SUPPLY_SSM_"):
			if err := a.AddSupply("ssm", pos[0], pos[1]); err != nil {
				return err
			}
		case strings.HasPrefix(kv[0], "AIRBORNE_SUPPLY_S3_SYNC_"):
			if err := a.AddSupply("s3sync", pos[0], pos[1]); err != nil {
				return err
			}
		case strings.HasPrefix(kv[0], "AIRBORNE_SUPPLY_S3_"):
			if err := a.AddSupply("s3", pos[0], pos[1]); err != nil {
				return err
			}
		default:
			return fmt.Errorf("No provider was found to match the given supply: %s", kv[0])
		}
	}
	return nil
}
