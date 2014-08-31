package net.SubredditAnalytics.Jobs;

import org.apache.hadoop.mapreduce.Job;

import java.io.IOException;

/**
 * Created by tom on 8/30/14.
 */
public interface MRJobFactory {
    public Job makeMapReduceJob() throws IOException;
}
