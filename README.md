# SubredditAnalytics Hadoop MR Jobs
[![Build Status](https://travis-ci.org/TomDunn/SubredditAnalyticsHadoopMRJobs.svg?branch=master)](https://travis-ci.org/TomDunn/SubredditAnalyticsHadoopMRJobs)

Jobs for transforming data logged from Reddit into useful analytics.

## Usage

```
./gradlew build
java -jar build/lib/<thejar>.jar tstData/ tmp/ouput
```

## AWS EMR Usage

Upload the jar somewhere in S3 (s3JarLoc). Have an input bucket folder and an output bucket folder (s3In s3Out, can be in same bucket)

1. Upload the jar somewhere into S3 (s3JarLoc)
2. Create an input bucket folder (s3In)
3. Create an ouput bucket folder (s3Out), can be in same bucket as above as long as folders are distinct.
4. Create an EMR job and specify custom JAR step using the s3JarLoc. It should take two arguments s3In and s3Out
5. Run!

Make sure that s3Out does not exist before the job is run, this will fail the job.

## TODO

1. Resolve the MRUnit issue, currently I have hardcoded the jar into the repo. MVN needs to fix the 404 issue when downloading MRUnit.
2. Address some of the TODOs in comments.
3. Add more analytics jobs.
