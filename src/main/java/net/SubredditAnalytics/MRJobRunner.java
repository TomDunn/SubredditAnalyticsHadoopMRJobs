package net.SubredditAnalytics;

import net.SubredditAnalytics.HourlyPostCount.HourlyPostCountJobFactory;
import net.SubredditAnalytics.Jobs.MRJobFactory;
import net.SubredditAnalytics.SubredditLinkDomainCount.SubredditLinkDomainCountJobFactory;
import net.SubredditAnalytics.TopDomains.TopDomainJobFactory;
import net.SubredditAnalytics.UniquePostFilter.UniquePostFilterJobFactory;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by tom on 8/30/14.
 */
public class MRJobRunner {
    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        if (args.length < 3) {
            throw new IllegalArgumentException("Three arguments are required: JobName InPath OutPath");
        }

        final Map<String, MRJobFactory> jobNameToFactory = new HashMap<String, MRJobFactory>();

        jobNameToFactory.put("filterPosts", new UniquePostFilterJobFactory());
        jobNameToFactory.put("hourlyPostCounts", new HourlyPostCountJobFactory());
        jobNameToFactory.put("subredditLinkDomainCount", new SubredditLinkDomainCountJobFactory());
        jobNameToFactory.put("topDomains", new TopDomainJobFactory());

        final String jobName = args[0];

        if (!jobNameToFactory.containsKey(jobName)) {
            throw new IllegalArgumentException("No such JobName: " + jobName);
        }

        Job job = jobNameToFactory.get(jobName).makeMapReduceJob();

        FileInputFormat.addInputPath(job, new Path(args[1]));
        FileOutputFormat.setOutputPath(job, new Path(args[2]));

        job.waitForCompletion(true);
    }
}
