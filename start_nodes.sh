#!/bin/bash

# format the alluxio fs
alluxio format
## remove the logs
rm -rf logs/*
## stop and format all
./bin/alluxio-stop.sh all
alluxio format
## start the stuff
./bin/alluxio-start.sh -a master
./bin/alluxio-start.sh -a worker SudoMount
./bin/alluxio-start.sh -a job_master
./bin/alluxio-start.sh -a job_worker
