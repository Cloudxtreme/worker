package main

import (
	"github.com/lavab/worker/shared"
)

type Task interface {
	Run(*shared.Job) error
}
