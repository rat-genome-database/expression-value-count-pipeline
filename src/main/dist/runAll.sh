#!/usr/bin/env bash
. /etc/profile
APPNAME=expression-value-count-pipeline
SERVER=`hostname -s | tr '[a-z]' '[A-Z]'`

APPDIR=/home/rgddata/pipelines/$APPNAME
cd $APPDIR

java -Dspring.config=$APPDIR/../properties/default_db2.xml \
    -Dlog4j.configurationFile=file://$APPDIR/properties/log4j2.xml \
    -jar lib/$APPNAME.jar -runForAll "$@" > run.log 2>&1

mailx -s "[$SERVER] expression-value-count-pipeline Run" llamers@mcw.edu < $APPDIR/logs/summary.log
