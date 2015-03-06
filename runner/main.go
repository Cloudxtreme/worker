package main

import (
	"encoding/json"
	"os"
	"runtime"
	"time"

	"github.com/Sirupsen/logrus"
	"github.com/bitly/go-nsq"
	"github.com/dancannon/gorethink"
	"github.com/lavab/flag"
	"github.com/lavab/worker/shared"
)

var (
	// Enable config functionality
	configFlag = flag.String("config", "", "Config file to read")
	// General flags
	logFormatterType = flag.String("log", "text", "Log formatter type")
	forceColors      = flag.Bool("force_colors", false, "Force colored prompt?")
	// etcd
	etcdAddress  = flag.String("etcd_address", "", "etcd peer addresses split by commas")
	etcdCAFile   = flag.String("etcd_ca_file", "", "etcd path to server cert's ca")
	etcdCertFile = flag.String("etcd_cert_file", "", "etcd path to client cert file")
	etcdKeyFile  = flag.String("etcd_key_file", "", "etcd path to client key file")
	etcdPath     = flag.String("etcd_path", "worker/manager/", "Path of the keys")
	// Database-related flags
	rethinkdbAddress = flag.String("rethinkdb_address", func() string {
		address := os.Getenv("RETHINKDB_PORT_28015_TCP_ADDR")
		if address == "" {
			address = "127.0.0.1"
		}
		return address + ":28015"
	}(), "Address of the RethinkDB database")
	rethinkdbKey      = flag.String("rethinkdb_key", os.Getenv("RETHINKDB_AUTHKEY"), "Authentication key of the RethinkDB database")
	rethinkdbDatabase = flag.String("rethinkdb_db", func() string {
		database := os.Getenv("RETHINKDB_DB")
		if database == "" {
			database = "worker_dev"
		}
		return database
	}(), "Database name on the RethinkDB server")
	// lookupd address
	lookupdAddress = flag.String("lookupd_address", func() string {
		address := os.Getenv("NSQLOOKUPD_PORT_4161_TCP_ADDR")
		if address == "" {
			address = "127.0.0.1"
		}
		return address + ":4161"
	}(), "Address of the lookupd server")
)

var (
	tasks map[string]Task
)

func main() {
	// Parse the flags
	flag.Parse()

	// Initialize a new logger
	log := logrus.New()
	if *logFormatterType == "text" {
		log.Formatter = &logrus.TextFormatter{
			ForceColors: *forceColors,
		}
	} else if *logFormatterType == "json" {
		log.Formatter = &logrus.JSONFormatter{}
	}
	log.Level = logrus.DebugLevel

	// Initialize a database connection
	session, err := gorethink.Connect(gorethink.ConnectOpts{
		Address: *rethinkdbAddress,
		AuthKey: *rethinkdbKey,
		MaxIdle: 10,
		Timeout: time.Second * 10,
	})
	if err != nil {
		log.WithFields(logrus.Fields{
			"error": err.Error(),
		}).Fatal("Unable to connect to RethinkDB")
	}

	// Fetch the scripts
	cursor, err := gorethink.Db(*rethinkdbDatabase).Table("scripts").Run(session)
	if err != nil {
		log.WithFields(logrus.Fields{
			"error": err.Error(),
		}).Fatal("Unable to fetch scripts from RethinkDB")
	}
	var scripts []*shared.Script
	if err := cursor.All(&scripts); err != nil {
		log.WithFields(logrus.Fields{
			"error": err.Error(),
		}).Fatal("Unable to fetch scripts from RethinkDB")
	}

	// Load them
	for _, script := range scripts {
		if script.Interpreter == "js" {
			tasks[script.ID] = &JSTask{
				ID:     script.ID,
				Source: script.Source,
			}
		} /* else if script.Interpreter == "lua" {
			tasks[script.ID] = &LuaTask{
				ID:     script.ID,
				Source: script.Source,
			}
		}*/
	}

	// Queue for jobs
	hostname, err := os.Hostname()
	if err != nil {
		log.WithFields(logrus.Fields{
			"error": err,
		}).Fatal("Unable to get the hostname")
	}
	consumer, err := nsq.NewConsumer("jobs", hostname, nsq.NewConfig())
	if err != nil {
		log.WithFields(logrus.Fields{
			"error": err.Error(),
		}).Fatal("Unable to create a new consumer")
	}
	consumer.AddConcurrentHandlers(nsq.HandlerFunc(func(m *nsq.Message) error {
		// Decode the job
		var job *shared.Job
		if err := json.Unmarshal(m.Body, &job); err != nil {
			return err
		}

		// Ensure that we have such job
		if task, ok := tasks[job.Name]; ok {
			// Run it
			if err := task.Run(job); err != nil {
				m.Requeue(-1)
				return err
			}

			m.Finish()
			return nil
		}

		// We failed.
		m.Requeue(-1)
		return nil
	}), runtime.GOMAXPROCS(0))
	if err := consumer.ConnectToNSQLookupd(*lookupdAddress); err != nil {
		log.WithFields(logrus.Fields{
			"error": err.Error(),
		}).Fatal("Unable to connect to the lookupd")
	}

	log.Print("Loaded all scripts.")
	log.Print("Starting the watcher.")

	// Watch for changes
	cursor, err = gorethink.Db(*rethinkdbDatabase).Table("scripts").Changes().Run(session)
	var change struct {
		NewValue *shared.Script `gorethink:"new_val"`
		OldValue *shared.Script `gorethink:"old_val"`
	}
	for cursor.Next(&change) {
		if change.NewValue == nil {
			// Remove a job
			if _, ok := tasks[change.OldValue.ID]; ok {
				delete(tasks, change.OldValue.ID)
			}
		} else if change.OldValue == nil {
			if change.NewValue.Interpreter == "js" {
				tasks[change.NewValue.ID] = &JSTask{
					ID:     change.NewValue.ID,
					Source: change.NewValue.Source,
				}
			} /* else if change.NewValue.Interpreter == "lua" {
				tasks[change.NewValue.ID] = &LuaTask{
					ID:     change.NewValue.ID,
					Source: change.NewValue.Source,
				}
			}*/
		} else {
			if _, ok := tasks[change.OldValue.ID]; ok {
				delete(tasks, change.OldValue.ID)
			}

			if change.NewValue.Interpreter == "js" {
				tasks[change.NewValue.ID] = &JSTask{
					ID:     change.NewValue.ID,
					Source: change.NewValue.Source,
				}
			} /* else if change.NewValue.Interpreter == "lua" {
				tasks[change.NewValue.ID] = &LuaTask{
					ID:     change.NewValue.ID,
					Source: change.NewValue.Source,
				}
			}*/
		}
	}
}
