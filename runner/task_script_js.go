package main

import (
	"log"
	"strconv"
	"strings"
	"time"

	"github.com/dancannon/gorethink"
	"github.com/lavab/worker/shared"
	"github.com/robertkrimen/otto"
	_ "github.com/robertkrimen/otto/underscore"
)

func convertObject(input *otto.Object) map[string]interface{} {
	output := map[string]interface{}{}
	if input == nil {
		return output
	}

	for _, key := range input.Keys() {
		v1, _ := input.Get(key)

		var trueValue interface{}
		if v1.IsBoolean() {
			trueValue, _ = v1.ToBoolean()
		} else if v1.IsNumber() {
			trueValue, _ = v1.ToInteger()
		} else {
			trueValue = v1.String()
		}

		output[key] = trueValue
	}

	return output
}

func enrichFilter(input map[string]interface{}) {
	for k, v1 := range input {
		if v2, ok := v1.(string); ok {
			if strings.HasPrefix(v2, "not~") {
				if len(v2) > 4 {
					input[k] = gorethink.Not(gorethink.Expr(v2[5:]))
				}
			} else if strings.HasPrefix(v2, "lt~") && len(v2) > 3 {
				if v2[3:] == "now" {
					input[k] = gorethink.Lt(gorethink.Now())
				} else {
					t, err := time.Parse(time.RFC3339, v2[3:])
					if err == nil {
						input[k] = gorethink.Lt(gorethink.Expr(t))
					} else {
						i, err := strconv.Atoi(v2[3:])
						if err == nil {
							input[k] = gorethink.Lt(gorethink.Expr(i))
						}
					}
				}
			} else if strings.HasPrefix(v2, "le~") && len(v2) > 3 {
				if v2[3:] == "now" {
					input[k] = gorethink.Le(gorethink.Now())
				} else {
					t, err := time.Parse(time.RFC3339, v2[3:])
					if err == nil {
						input[k] = gorethink.Le(gorethink.Expr(t))
					} else {
						i, err := strconv.Atoi(v2[3:])
						if err == nil {
							input[k] = gorethink.Le(gorethink.Expr(i))
						}
					}
				}
			} else if strings.HasPrefix(v2, "gt~") && len(v2) > 3 {
				if v2[3:] == "now" {
					input[k] = gorethink.Gt(gorethink.Now())
				} else {
					t, err := time.Parse(time.RFC3339, v2[3:])
					if err == nil {
						input[k] = gorethink.Gt(gorethink.Expr(t))
					} else {
						i, err := strconv.Atoi(v2[3:])
						if err == nil {
							input[k] = gorethink.Gt(gorethink.Expr(i))
						}
					}
				}
			} else if strings.HasPrefix(v2, "ge~") && len(v2) > 3 {
				if v2[3:] == "now" {
					input[k] = gorethink.Ge(gorethink.Now())
				} else {
					t, err := time.Parse(time.RFC3339, v2[3:])
					if err == nil {
						input[k] = gorethink.Ge(gorethink.Expr(t))
					} else {
						i, err := strconv.Atoi(v2[3:])
						if err == nil {
							input[k] = gorethink.Ge(gorethink.Expr(i))
						}
					}
				}
			}
		}
	}
}

var jsEnv = map[string]interface{}{
	"rethinkDelete": func(call otto.FunctionCall) otto.Value {
		var (
			db        = call.Argument(0).String()
			table     = call.Argument(1).String()
			condition = call.Argument(2).Object()
		)

		if condition == nil {
			log.Print("Invalid args")
			return otto.FalseValue()
		}

		cond := convertObject(condition)
		enrichFilter(cond)

		_, err := gorethink.Db(db).Table(table).Filter(cond).Delete().Run(session)
		if err != nil {
			log.Print(err)
			return otto.FalseValue()
		}

		return otto.TrueValue()
	},
	"rethinkUpdate": func(call otto.FunctionCall) otto.Value {
		var (
			db        = call.Argument(0).String()
			table     = call.Argument(1).String()
			condition = call.Argument(2).Object()
			change    = call.Argument(3).Object()
		)

		if condition == nil {
			log.Print("Invalid args")
			return otto.FalseValue()
		}

		cond := convertObject(condition)
		enrichFilter(cond)
		chng := convertObject(change)

		_, err := gorethink.Db(db).Table(table).Filter(cond).Update(chng).Run(session)
		if err != nil {
			log.Print(err)
			return otto.FalseValue()
		}

		return otto.TrueValue()
	},
	"rethinkInsert": func(call otto.FunctionCall) otto.Value {
		var (
			db    = call.Argument(0).String()
			table = call.Argument(1).String()
			input = call.Argument(2).Object()
		)

		if input == nil {
			log.Print("Invalid args")
			return otto.FalseValue()
		}

		obj := convertObject(input)

		_, err := gorethink.Db(db).Table(table).Insert(obj).Run(session)
		if err != nil {
			log.Print(err)
			return otto.FalseValue()
		}

		return otto.TrueValue()
	},
	"rethinkGet": func(call otto.FunctionCall) otto.Value {
		var (
			db        = call.Argument(0).String()
			table     = call.Argument(1).String()
			condition = call.Argument(2).Object()
		)

		cond := convertObject(condition)
		enrichFilter(cond)

		cursor, err := gorethink.Db(db).Table(table).Filter(cond).Run(session)
		if err != nil {
			log.Print(err)
			return otto.FalseValue()
		}

		var result []map[string]interface{}
		if err := cursor.All(&result); err != nil {
			log.Print(err)
			return otto.FalseValue()
		}

		ottoResult, err := otto.ToValue(result)
		if err != nil {
			log.Print(err)
			return otto.FalseValue()
		}

		return ottoResult
	},
}

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

	for k, v := range jsEnv {
		if err := vm.Set(k, v); err != nil {
			return err
		}
	}

	if _, err := vm.Run(j.Source); err != nil {
		return err
	}

	return nil
}
