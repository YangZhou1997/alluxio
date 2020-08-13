#!/bin/bash

rm -rf $HOME/alluxio
cp -r $HOME/alluxio_vscode $HOME/alluxio

cd $HOME/alluxio
mvn eclipse:clean
mvn -T 4C clean install -DskipTests -Dmaven.javadoc.skip -Dfindbugs.skip -Dcheckstyle.skip -Dlicense.skip
