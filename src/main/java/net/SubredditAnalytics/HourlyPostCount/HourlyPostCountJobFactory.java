package net.SubredditAnalytics.HourlyPostCount;

import net.SubredditAnalytics.Jobs.MRJobFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;

/**
 * Created by tom on 8/30/14.
 */
public class HourlyPostCountJobFactory implements MRJobFactory {
    public Job makeMapReduceJob() throws IOException {
        Configuration conf = new Configuration();

        Job job = new Job(conf, "hourlyPostCount");
        job.setJarByClass(HourlyPostCountJobFactory.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        job.setMapperClass(HourlyPostCountMapper.class);
        job.setReducerClass(HourlyPostCountReducer.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        return job;
    }
}
