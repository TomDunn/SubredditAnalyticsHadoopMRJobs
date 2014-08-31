OUTPATH='tmp/out'
INPATH='tstData/'

JAR='build/libs/SubredditAnalyticsMRJobs-0.0.1.jar'

rm -rf $OUTPATH

java -jar $JAR  $INPATH "$OUTPATH/UNIQ_POSTS"
