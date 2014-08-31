#!/bin/bash

OUTPATH='tmp/out'
INPATH='tstData/'

JAR='build/libs/SubredditAnalyticsMRJobs-0.0.1.jar'

mkdir   -p $OUTPATH
rm      -rf $OUTPATH
mkdir   -p $OUTPATH

java -jar $JAR filterPosts $INPATH "$OUTPATH/UNIQ_POSTS"
java -jar $JAR hourlyPostCounts "$OUTPATH/UNIQ_POSTS" "$OUTPATH/HOURLY_POST_COUNTS"
java -jar $JAR subredditLinkDomainCount "$OUTPATH/UNIQ_POSTS" "$OUTPATH/SUBREDDIT_LINK_DOMAIN_COUNTS"
