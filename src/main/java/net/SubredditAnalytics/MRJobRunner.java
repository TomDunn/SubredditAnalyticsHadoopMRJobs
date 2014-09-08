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
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;

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

        if (args[0].equals("RUN_ALL")) {
            MRJobRunner.runAll(new Path(args[1]), new Path(args[2]));
            return;
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

    public static void runAll(final Path inPath, final Path baseOutPath) throws ClassNotFoundException, InterruptedException, IOException {
        DateTime dateTime = new DateTime(DateTimeZone.UTC);

        final String nowTimestamp = Long.toString(dateTime.now().getMillis() / 1000L);

        /* Unique Post Job */
        Job uniquePostJob = (new UniquePostFilterJobFactory()).makeMapReduceJob();
        Path uniquePostOutPath = new Path(baseOutPath + "/UNIQUE_POSTS/" + nowTimestamp);

        FileInputFormat.addInputPath(uniquePostJob, inPath);
        FileOutputFormat.setOutputPath(uniquePostJob, uniquePostOutPath);

        uniquePostJob.waitForCompletion(true);

        /* Hourly post count jobs */
        Job hourlyPostCountJob = (new HourlyPostCountJobFactory()).makeMapReduceJob();
        Path hourlyPostCountOutPath = new Path(baseOutPath + "/HOURLY_POST_COUNTS/" + nowTimestamp);

        FileInputFormat.addInputPath(hourlyPostCountJob, uniquePostOutPath);
        FileOutputFormat.setOutputPath(hourlyPostCountJob, hourlyPostCountOutPath);

        hourlyPostCountJob.waitForCompletion(true);

        /* Domain counts */
        Job postDomainCountJob = (new SubredditLinkDomainCountJobFactory()).makeMapReduceJob();
        Path postDomainCountOutPath = new Path(baseOutPath + "/DOMAIN_COUNTS/" + nowTimestamp);

        FileInputFormat.addInputPath(postDomainCountJob, uniquePostOutPath);
        FileOutputFormat.setOutputPath(postDomainCountJob, postDomainCountOutPath);

        postDomainCountJob.waitForCompletion(true);

        /* Top Domains */
        Job topDomainJob = (new TopDomainJobFactory()).makeMapReduceJob();
        Path topDomainOutPath = new Path(baseOutPath + "/TOP_DOMAINS/" + nowTimestamp);

        FileInputFormat.addInputPath(topDomainJob, postDomainCountOutPath);
        FileOutputFormat.setOutputPath(topDomainJob, topDomainOutPath);

        topDomainJob.waitForCompletion(true);
    }
}
