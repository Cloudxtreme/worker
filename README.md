# worker

<img src="https://mail.lavaboom.com/img/Lavaboom-logo.svg" align="right" width="200px" />

Task runner behind Lavaboom's systems. 

Scheduler fetches jobs list from RethinkDB (and listens to changes in the
database) and adds them to an internal timer that supports most of the cron
syntax.

Runner receives jobs to run from NSQ, looks them up in an internal jobs list
and executes the adequate task. Tasks can be either written in Go (by directly
modifying the runner's code) or by using an otto-based JS virtual machine that
automatically syncs the scripts with a scripts table in the RethinkDB database.

## Requirements

 - RethinkDB
 - NSQ

## How it works

<img src="http://i.imgur.com/q3oVBR3.png">

## Usage

### Inside a Docker container

**This image will be soon uploaded to Docker Hub.**

```bash
git clone git@github.com:lavab/worker.git
cd worker
docker build -t "lavab/worker" .

# Start the scheduler
docker run \
    -e "RETHINKDB_ADDRESS=172.8.0.1:28015" \
    -e "RETHINKDB_DB=worker" \
    -e "NSQD_ADDRESS=172.8.0.1:4150" \
    --name scheduler \
    lavab/worker \
    scheduler

# Start the runner
docker run \
    -e "RETHINKDB_ADDRESS=172.8.0.1:28015" \
    -e "RETHINKDB_DB=worker" \
    -e "LOOKUPD_ADDRESS=172.8.0.1:4161" \
    --name runner \
    lavab/worker \
    runner
```

### Directly running the parts

```bash
go get github.com/lavab/worker/scheduler
go get github.com/lavab/worker/runner

# Start the scheduler
scheduler \
    --rethinkdb_address=172.8.0.1:28015 \
    --rethinkdb_db=worker \
    --nsqd_address=172.8.0.1:4150

# Start the runner
runner \
    --rethinkdb_address=172.8.0.1:28015 \
    --rethinkdb_db=worker \
    --lookupd_address=172.8.0.1:4161
```

## License

This project is licensed under the MIT license. Check `license` for more
information.