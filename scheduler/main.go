package main

import (
	"encoding/json"
	"os"
	"time"

	"github.com/Sirupsen/logrus"
	"github.com/bitly/go-nsq"
	"github.com/dancannon/gorethink"
	"github.com/lavab/flag"
	"github.com/lavab/worker/shared"
	"gopkg.in/robfig/cron.v2"
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
	// nsq address
	nsqdAddress = flag.String("nsqd_address", func() string {
		address := os.Getenv("NSQD_PORT_4150_TCP_ADDR")
		if address == "" {
			address = "127.0.0.1"
		}
		return address + ":4150"
	}(), "Address of the nsqd server")
)

var (
	log      *logrus.Logger
	producer *nsq.Producer

	mapping = map[string]cron.EntryID{}
)

type Job struct {
	shared.Job
}

func (j *shared.Job) Run() {
	body, err := json.Marshal(j)
	if err != nil {
		log.WithFields(logrus.Fields{
			"id":   j.ID,
			"name": j.Name,
		}).Fatal("Unable to encode a job notification")
	}

	if err := producer.Publish("jobs", body); err != nil {
		log.WithFields(logrus.Fields{
			"id":   j.ID,
			"name": j.Name,
		}).Fatal("Unable to queue a job")
	}
}

func main() {
	flag.Parse()

	// Initialize a new logger
	log = logrus.New()
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

	// Connect to nsq
	producer, err = nsq.NewProducer(*nsqdAddress, nsq.NewConfig())
	if err != nil {
		log.WithFields(logrus.Fields{
			"error": err.Error(),
		}).Fatal("Unable to connect to NSQd")
	}

	// Fetch the jobs
	cursor, err := gorethink.Db(*rethinkdbDatabase).Table("jobs").Run(session)
	if err != nil {
		log.WithFields(logrus.Fields{
			"error": err.Error(),
		}).Fatal("Unable to fetch jobs from RethinkDB")
	}
	var jobs []*shared.Job
	if err := cursor.All(&jobs); err != nil {
		log.WithFields(logrus.Fields{
			"error": err.Error(),
		}).Fatal("Unable to fetch jobs from RethinkDB")
	}

	// Create a new cron runner
	runner := cron.New()
	for _, job := range jobs {
		id, err := runner.AddJob(job.When, job)
		if err != nil {
			log.WithFields(logrus.Fields{
				"error": err.Error(),
			}).Fatal("Unable to queue a job")
		}

		mapping[job.ID] = id
	}

	log.Print("Starting the runner")

	runner.Start()

	log.Print("Starting the change watcher")

	// Watch for changes
	cursor, err = gorethink.Db(*rethinkdbDatabase).Table("jobs").Changes().Run(session)

	var change struct {
		NewValue *shared.Job `gorethink:"new_val"`
		OldValue *shared.Job `gorethink:"old_val"`
	}
	for cursor.Next(&change) {
		if change.NewValue == nil {
			// Remove a job
			if id, ok := mapping[change.OldValue.ID]; ok {
				runner.Remove(id)
			}
		} else if change.OldValue == nil {
			// Create a new job
			id, err := runner.AddJob(change.NewValue.When, change.NewValue)
			if err != nil {
				log.WithFields(logrus.Fields{
					"error": err.Error(),
				}).Fatal("Unable to queue a job")
			}
			mapping[change.NewValue.ID] = id
		} else {
			// Recreate a job
			if id, ok := mapping[change.OldValue.ID]; ok {
				runner.Remove(id)
			}

			// Create a new job
			id, err := runner.AddJob(change.NewValue.When, change.NewValue)
			if err != nil {
				log.WithFields(logrus.Fields{
					"error": err.Error(),
				}).Fatal("Unable to queue a job")
			}
			mapping[change.NewValue.ID] = id
		}
	}
}
