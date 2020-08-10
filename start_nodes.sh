#!/bin/bash


## remove the logs
rm -rf logs/*
## start the stuff
./bin/alluxio-start.sh -a master
./bin/alluxio-start.sh -a worker SudoMount
./bin/alluxio-start.sh -a job_master
./bin/alluxio-start.sh -a job_worker
