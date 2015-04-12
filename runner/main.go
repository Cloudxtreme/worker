package main

import (
	"encoding/json"
	"os"
	"runtime"
	"strings"

	"github.com/Sirupsen/logrus"
	"github.com/bitly/go-nsq"
	r "github.com/dancannon/gorethink"
	"github.com/lavab/kiri"
	"github.com/lavab/worker/shared"
	"github.com/namsral/flag"
)

var (
	// General flags
	logFormatterType = flag.String("log", "text", "Log formatter type")
	forceColors      = flag.Bool("force_colors", false, "Force colored prompt?")
	// Database-related flags
	rethinkdbDatabase = flag.String("rethinkdb_db", func() string {
		database := os.Getenv("RETHINKDB_DB")
		if database == "" {
			database = "worker_dev"
		}
		return database
	}(), "Database name on the RethinkDB server")

	kiriAddresses           = flag.String("kiri_addresses", "", "Addresses of the etcd servers to use")
	kiriDiscoveryStores     = flag.String("kiri_discovery_stores", "", "Stores list for service discovery. Syntax: kind,path;kind,path")
	kiriDiscoveryRethinkDB  = flag.String("kiri_discovery_rethinkdb", "rethinkdb", "Name of the RethinkDB service in SD")
	kiriDiscoveryNSQLookupd = flag.String("kiri_discovery_nsqlookupd", "nsqlookupd-http", "Name of the nsqlookupd HTTP server in SD")
)

var (
	tasks = map[string]Task{
		"clear_expired_tokens": &ClearExpiredTokens{},
	}
	session *r.Session
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

	// Create the database and a scripts table
	r.DbCreate(*rethinkdbDatabase).Exec(session)
	r.Db(*rethinkdbDatabase).TableCreate("scripts").Exec(session)

	// Fetch the scripts
	cursor, err := r.Db(*rethinkdbDatabase).Table("scripts").Run(session)
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
	// Create a new consumer
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

	if err = kd.Discover(*kiriDiscoveryNSQLookupd, nil, kiri.DiscoverFunc(func(service *kiri.Service) error {
		return consumer.ConnectToNSQLookupd(service.Address)
	})); err != nil {
		log.WithFields(logrus.Fields{
			"error": err.Error(),
		}).Fatal("Unable to connect to the lookupd")
	}

	log.Print("Loaded all scripts.")
	log.Print("Starting the watcher.")

	// Watch for changes
	cursor, err = r.Db(*rethinkdbDatabase).Table("scripts").Changes().Run(session)
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
