package net.SubredditAnalytics.UniquePostFilter;

import net.SubredditAnalytics.Jobs.MRJobFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;

/**
 * Created by tom on 8/30/14.
 */
public class UniquePostFilterJobFactory implements MRJobFactory {
    public Job makeMapReduceJob() throws IOException {
        Configuration conf = new Configuration();

        Job job = new Job(conf, "postUniqueFilter");
        job.setJarByClass(UniquePostFilterJobFactory.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setMapperClass(UniquePostMapper.class);
        job.setReducerClass(UniquePostReducer.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        return job;
    }
}