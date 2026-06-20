package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strings"

	"github.com/minikocha/airborne"
	"github.com/minikocha/airborne/handlers/s3"
	"github.com/minikocha/airborne/handlers/ssm"
)

func main() {
	app := airborne.New()

	ssm, _ := ssm.New()
	app.AddHandler(ssm)

	s3, _ := s3.New()
	app.AddHandler(s3)

	if err := loadEnvVars(app); err != nil {
		log.Fatal(err)
	}

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt)
	defer cancel()
	if err := app.Run(ctx, os.Args); err != nil {
		log.Fatal(err)
	}
}

func loadEnvVars(app *airborne.App) error {
	for _, e := range os.Environ() {
		if !strings.HasPrefix(e, "AIRBORNE_ASSOC_") {
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
		case strings.HasPrefix(kv[0], "AIRBORNE_ASSOC_SSM_"):
			if err := app.AddAssoc("ssm", pos[0], pos[1]); err != nil {
				return err
			}
		case strings.HasPrefix(kv[0], "AIRBORNE_ASSOC_S3_"):
			if err := app.AddAssoc("s3", pos[0], pos[1]); err != nil {
				return err
			}
		default:
			return fmt.Errorf("No provider was found to match the given supply: %s", kv[0])
		}
	}

	return nil
}
