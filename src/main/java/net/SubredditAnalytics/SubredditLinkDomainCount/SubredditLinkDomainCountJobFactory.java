package net.SubredditAnalytics.SubredditLinkDomainCount;

import net.SubredditAnalytics.Jobs.MRJobFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;

/**
 * Created by tom on 8/31/14.
 */
public class SubredditLinkDomainCountJobFactory implements MRJobFactory {
    public Job makeMapReduceJob() throws IOException {
        Configuration conf = new Configuration();

        Job job = new Job(conf, "subredditLinkDomainCount");
        job.setJarByClass(SubredditLinkDomainCountJobFactory.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        job.setMapperClass(SubredditLinkDomainCountMapper.class);
        job.setReducerClass(SubredditLinkDomainCountReducer.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        return job;
    }
}
