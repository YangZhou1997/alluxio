#!/usr/bin/env bash

source ~/.profile
mvn clean install -Dlicense.skip=true -Phadoop-2 -Dhadoop.version=2.8.5 -DskipTests -Dcheckstyle.skip -Dmaven.test.skip=true
cd ~/bigdata-conf/ && ./sync_conf.sh

alluxio-stop.sh all
alluxio format
alluxio-start.sh all SudoMount

# Enable writing data into alluxio: Two Ways to Keep Files in Sync Between Alluxio and HDFS
alluxio fs ls -R -Dalluxio.user.file.metadata.sync.interval=0 /user/hive/warehouse/sqlres

cd ~/spark-tpc-ds-performance-test && python run_all.py