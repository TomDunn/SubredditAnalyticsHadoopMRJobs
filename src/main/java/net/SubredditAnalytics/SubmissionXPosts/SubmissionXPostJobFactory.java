package net.SubredditAnalytics.SubmissionXPosts;

import net.SubredditAnalytics.SubredditLinkDomainCount.SubredditLinkDomainCountMapper;
import net.SubredditAnalytics.SubredditLinkDomainCount.SubredditLinkDomainCountReducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;

/**
 * Created by tom on 9/28/14.
 */
public class SubmissionXPostJobFactory {
    public Job makeMapReduceJob() throws IOException {
        Configuration conf = new Configuration();

        Job job = new Job(conf, "submissionXPostCount");
        job.setJarByClass(SubmissionXPostMapper.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        job.setMapperClass(SubmissionXPostMapper.class);
        job.setReducerClass(SubmissionXPostReducer.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        return job;
    }
}
