package main

import (
	"github.com/lavab/worker/shared"
	"github.com/robertkrimen/otto"
)

type JSTask struct {
	ID     string
	Source string
}

func (j *JSTask) Run(job *shared.Job) error {
	vm := otto.New()
	if err := vm.Set("id", j.ID); err != nil {
		return err
	}

	if err := vm.Set("job", job); err != nil {
		return err
	}

	if _, err := vm.Run(j.Source); err != nil {
		return err
	}

	return nil
}
