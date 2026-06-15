package airborne

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
)

const (
	defaultGracePeriod = 0
)

type App struct {
	mappings map[string]Handler
}

func NewApp() *App {
	return &App{
		mappings: make(map[string]Handler),
	}
}

func (app *App) AddHandler(handler Handler) {
	app.mappings[handler.Type()] = handler
}

func (app *App) AddSupply(handlerType string, src string, dest string) error {
	if _, ok := app.mappings[handlerType]; !ok {
		return fmt.Errorf("No handler matching the given type was found: %s", handlerType)
	}

	return app.mappings[handlerType].Add(src, dest)
}

func (app *App) Run(ctx context.Context, args []string) error {
	if len(app.mappings) == 0 {
		return nil
	}

	slog.Info("airborne: running")

	opts := Parse(args)
	if opts.Version {
		fmt.Printf("versin: %s\n", Version())
		return nil
	}

	ch := app.run(ctx, opts)
	select {
	case err := <-ch:
		if err != nil {
			return err
		}
	case <-ctx.Done():
		return ErrInterrupted // NOTE: when perform gracefull shutdown: app.cancel(ch)
	}

	slog.Info("airborne: fineshed")
	return nil
}

func (app *App) run(ctx context.Context, opts *Options) chan error {
	ch := make(chan error, 1)
	go func() {
		defer close(ch)

		var wg sync.WaitGroup
		sem := make(chan struct{}, opts.Concurrency)
		defer close(sem)

		for _, p := range app.mappings {
			select {
			case <-ctx.Done():
				wg.Wait()
				return
			case sem <- struct{}{}:
				wg.Go(func() {
					defer func() { <-sem }()

					p.SetConcurrency(opts.Concurrency) // TODO: 並列度を上げる
					if err := p.Run(ctx); err != nil {
						if !opts.ContinueOnError {
							// NOTE: to prevent panic: sending to a closed channel
							defer func() {
								if r := recover(); r != nil {
									return
								}
							}()
							ch <- err
							return
						}
						slog.Warn("error occuard but continue")
					}
				})
			}
		}
		wg.Wait()
	}()
	return ch
}

// NOTE: save this in case I need to perform gracefull shutdown in the future
//func (app *App) cancel(ch <-chan error) error {
//	ctx, cancel := context.WithTimeout(context.Background(), defaultGracePeriod*time.Second)
//	defer cancel()
//
//	select {
//	case <-ch:
//		return ErrInterrupted
//	case <-ctx.Done():
//		return ctx.Err()
//	}
//}
