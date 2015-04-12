package main

import (
	"encoding/json"
	"strings"

	"github.com/Sirupsen/logrus"
	"github.com/bitly/go-nsq"
	r "github.com/dancannon/gorethink"
	"github.com/lavab/kiri"
	"github.com/lavab/worker/shared"
	"github.com/namsral/flag"
	"gopkg.in/robfig/cron.v2"
)

var (
	logFormatterType  = flag.String("log", "text", "Log formatter type")
	forceColors       = flag.Bool("force_colors", false, "Force colored prompt?")
	rethinkdbDatabase = flag.String("rethinkdb_db", "worker_dev", "Database name on the RethinkDB server")

	kiriAddresses          = flag.String("kiri_addresses", "", "Addresses of the etcd servers to use")
	kiriDiscoveryStores    = flag.String("kiri_discovery_stores", "", "Stores list for service discovery. Syntax: kind,path;kind,path")
	kiriDiscoveryRethinkDB = flag.String("kiri_discovery_rethinkdb", "rethinkdb", "Name of the RethinkDB service in SD")
	kiriDiscoveryNSQd      = flag.String("kiri_discovery_nsqd", "nsqd-http", "Name of the nsqd HTTP server in SD")
)

var (
	log      *logrus.Logger
	producer *nsq.Producer

	mapping = map[string]cron.EntryID{}
)

type Job struct {
	shared.Job
}

func (j *Job) Run() {
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

	// Parse kiri addresses
	ka := strings.Split(*kiriAddresses, ",")

	// Set up kiri agent for discovery
	kd := kiri.New(ka)

	// Add stores to kd
	for i, store := range strings.Split(*kiriDiscoveryStores, ";") {
		parts := strings.Split(store, ",")
		if len(parts) != 2 {
			log.Fatalf("Invalid parts count in kiri_discovery_stores#%d", i)
		}

		var kind kiri.Format
		switch parts[0] {
		case "default":
			kind = kiri.Default
		case "puro":
			kind = kiri.Puro
		default:
			log.Fatalf("Invalid kind of store in kiri_discovery_stores#%d", i)
		}
		kd.Store(kind, parts[1])
	}

	// Connect to RethinkDB
	var session *r.Session
	err := kd.Discover(*kiriDiscoveryRethinkDB, nil, kiri.DiscoverFunc(func(service *kiri.Service) error {
		var err error
		session, err = r.Connect(r.ConnectOpts{
			Address: service.Address,
		})
		if err != nil {
			log.Print(err)
		}

		return err
	}))
	if err != nil {
		log.WithFields(logrus.Fields{
			"error": err.Error(),
		}).Fatal("Unable to connect to RethinkDB")
	}

	// Prepare database's structure
	r.DbCreate(*rethinkdbDatabase).Exec(session)
	r.Db(*rethinkdbDatabase).TableCreate("jobs").Exec(session)

	// Connect to nsq
	var producer *nsq.Producer
	err = kd.Discover(*kiriDiscoveryNSQd, nil, kiri.DiscoverFunc(func(service *kiri.Service) error {
		producer, err = nsq.NewProducer(service.Address, nsq.NewConfig())
		if err != nil {
			log.Error(err)
			return err
		}

		err = producer.Ping()
		if err != nil {
			log.Error(err)
			return err
		}

		return nil
	}))
	if err != nil {
		log.WithFields(logrus.Fields{
			"error": err.Error(),
		}).Fatal("Unable to connect to NSQd")
	}

	// Fetch the jobs
	cursor, err := r.Db(*rethinkdbDatabase).Table("jobs").Run(session)
	if err != nil {
		log.WithFields(logrus.Fields{
			"error": err.Error(),
		}).Fatal("Unable to fetch jobs from RethinkDB")
	}
	var jobs []*Job
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
	cursor, err = r.Db(*rethinkdbDatabase).Table("jobs").Changes().Run(session)

	var change struct {
		NewValue *Job `gorethink:"new_val"`
		OldValue *Job `gorethink:"old_val"`
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
