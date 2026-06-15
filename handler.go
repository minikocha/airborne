package airborne

import "context"

type Handler interface {
	Add(src string, dest string) error
	Run(ctx context.Context) error
	SetConcurrency(int) error
	Type() string
}
