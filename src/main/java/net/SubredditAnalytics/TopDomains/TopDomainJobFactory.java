package net.SubredditAnalytics.TopDomains;

import net.SubredditAnalytics.Jobs.MRJobFactory;
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
 * Created by tom on 8/31/14.
 */
public class TopDomainJobFactory implements MRJobFactory {
    public Job makeMapReduceJob() throws IOException {
        Configuration conf = new Configuration();

        Job job = new Job(conf, "topDomains");
        job.setJarByClass(TopDomainJobFactory.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setMapperClass(TopDomainMapper.class);
        job.setReducerClass(TopDomainReducer.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        return job;
    }
}
