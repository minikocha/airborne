package airborne

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
)

type App struct {
	mappings map[string]Handler
}

func New() *App {
	return &App{mappings: make(map[string]Handler)}
}

func (app *App) AddHandler(handler Handler) {
	app.mappings[handler.Type()] = handler
}

func (app *App) AddAssoc(handlerType string, src string, dst string) error {
	if _, ok := app.mappings[handlerType]; !ok {
		return fmt.Errorf("No handler matching the given type was found: %s", handlerType)
	}

	return app.mappings[handlerType].Add(src, dst)
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

	ch := make(chan error, 1)
	app.run(ctx, opts, ch)
	select {
	case err := <-ch:
		if err != nil {
			return err
		}
	case <-ctx.Done():
		return ErrInterrupted
	}

	slog.Info("airborne: finished")
	return nil
}

func (app *App) run(ctx context.Context, opts *Options, ch chan error) {
	go func() {
		defer close(ch)

		var wg sync.WaitGroup
		sem := make(chan struct{}, opts.Concurrency)
		defer close(sem)

		for _, h := range app.mappings {
			select {
			case <-ctx.Done():
				wg.Wait()
				return
			case sem <- struct{}{}:
				wg.Go(func() {
					defer func() { <-sem }()

					h.SetConcurrency(opts.Concurrency)
					if err := h.Run(ctx); err != nil {
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
}
