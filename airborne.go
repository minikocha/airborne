package airborne

import (
	"fmt"
	"log"
)

type Provider interface {
	Add(src string, dest string) error
	Output() error
}

type Airborne struct {
	providers map[string]Provider
}

func NewAirborne() *Airborne {
	return &Airborne{
		providers: make(map[string]Provider),
	}
}

func (a *Airborne) AddProvider(providerType string, provider Provider) {
	a.providers[providerType] = provider
}

func (a *Airborne) AddSupply(providerType string, src string, dest string) error {
	if _, ok := a.providers[providerType]; !ok {
		return fmt.Errorf("No provider was found for: %s", providerType)
	}
	return a.providers[providerType].Add(src, dest)
}

func (a *Airborne) Run() error {
	for k, v := range a.providers {
		log.Print(k)
		if err := v.Output(); err != nil {
			return err
		}
	}
	return nil
}
