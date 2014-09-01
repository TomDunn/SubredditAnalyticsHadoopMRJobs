package net.SubredditAnalytics.TopDomains;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created by tom on 8/31/14.
 */
public class TopDomainMapper extends Mapper<LongWritable, Text, Text, Text> {
    public void map(LongWritable k, Text value, Context context) throws InterruptedException, IOException {
        final String[] columns = value.toString().split("\t+");

        final String key = columns[0];

        final String subreddit = key.split("\\|")[0];
        final String domain    = key.split("\\|")[1];
        final int domainCount  = Integer.parseInt(columns[1]);

        context.write(new Text(subreddit), new Text(domain + "|" + Integer.toString(domainCount)));
        context.write(new Text(subreddit), new Text("_TOTAL_|" + Integer.toString(domainCount)));
    }
}
