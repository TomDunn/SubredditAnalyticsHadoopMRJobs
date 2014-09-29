#!/bin/bash

OUTPATH='tmp/out'
INPATH='tstData/'

JAR='build/libs/SubredditAnalyticsMRJobs-0.0.1.jar'

mkdir   -p $OUTPATH
rm      -rf $OUTPATH
mkdir   -p $OUTPATH

java -jar $JAR RUN_ALL $INPATH $OUTPATH
