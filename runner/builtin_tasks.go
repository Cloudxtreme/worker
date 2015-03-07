package main

import (
	"errors"

	"github.com/dancannon/gorethink"
	"github.com/lavab/worker/shared"
)

type ClearExpiredTokens struct{}

func (c *ClearExpiredTokens) Run(job *shared.Job) error {
	args, ok := job.Args.(map[string]interface{})
	if !ok {
		return errors.New("Invalid args")
	}

	idb, ok := args["db"]
	if !ok {
		return errors.New("Invalid args")
	}

	sdb, ok := idb.(string)
	if !ok {
		return errors.New("Invalid args")
	}

	_, err := gorethink.Db(sdb).Table("tokens").Filter(gorethink.Le(gorethink.Row.Field("expiry_date"), gorethink.Now())).Delete().Run(session)
	return err
}
