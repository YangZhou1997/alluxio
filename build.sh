#!/usr/bin/env bash

source ~/.profile

# rsync -r -l --exclude-from='exclude_me.txt' $HOME/alluxio_vscode/* $HOME/alluxio/
rm -rf $HOME/alluxio
cp -r $HOME/alluxio_vscode $HOME/alluxio

cd ~/alluxio

# remove .class .project .settings
mvn eclipse:clean

mvn clean install -Dlicense.skip=true -Phadoop-2 -Dhadoop.version=2.8.5 -DskipTests -Dcheckstyle.skip \
    -pl core,core/base,core/client,core/client/fs,core/client/hdfs,core/common,core/server,core/server/common,core/server/master,core/server/proxy,core/server/worker,core/transport,job,job/client,job/common,job/server,shaded/client,shaded/hadoop,underfs,underfs/alluxio

cd ~/bigdata-conf/ && ./sync_conf.sh

alluxio-stop.sh all
alluxio format
alluxio-start.sh all SudoMount

# Enable writing data into alluxio: Two Ways to Keep Files in Sync Between Alluxio and HDFS
alluxio fs ls -R -Dalluxio.user.file.metadata.sync.interval=0 /user/hive/warehouse/sqlres

# cd ~/spark-tpc-ds-performance-test && python run_all.py