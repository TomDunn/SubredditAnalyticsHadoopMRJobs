package net.SubredditAnalytics.SubredditsActivePastWeek;

import net.SubredditAnalytics.Jobs.MRJobFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;

/**
 * Created by tom on 10/4/14.
 */
public class SubredditsActivePastWeekJobFactory implements MRJobFactory {
    public Job makeMapReduceJob() throws IOException {
        Configuration configuration = new Configuration();

        Job job = new Job(configuration, "subreddits active in past week");
        job.setJarByClass(SubredditsActivePastWeekMapper.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        job.setMapperClass(SubredditsActivePastWeekMapper.class);
        job.setReducerClass(SubredditsActivePastWeekReducer.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        return job;
    }
}
