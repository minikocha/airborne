package airborne

import "flag"

const (
	defaultConcurrency     = 3
	defaultContinueOnError = false
)

type Options struct {
	Concurrency     int
	ContinueOnError bool
	Version         bool
}

func Parse(args []string) *Options {
	opts := &Options{}
	flags := createFlags(args, opts)
	flags.Parse(args[1:])
	return opts
}

func createFlags(args []string, opts *Options) *flag.FlagSet {
	flags := flag.NewFlagSet(args[0], flag.ExitOnError)

	flags.IntVar(
		&opts.Concurrency,
		"concurrency",
		defaultConcurrency,
		"set the number of processes that can be executed concurrently",
	)

	flags.BoolVar(
		&opts.ContinueOnError,
		"continue-on-error",
		defaultContinueOnError,
		"continue processing when an error occurs",
	)

	flags.BoolVar(
		&opts.Version,
		"version",
		false,
		"show version",
	)

	return flags
}
