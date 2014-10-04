package net.SubredditAnalytics.SubredditsActivePastWeek;

import net.SubredditAnalytics.Model.RedditPost;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.Period;

import java.io.IOException;

/**
 * Created by tom on 10/4/14.
 */
public class SubredditsActivePastWeekMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    static final DateTime now = new DateTime(DateTimeZone.UTC);
    static final DateTime oneWeekAgo = now.minusWeeks(1);
    static final IntWritable one = new IntWritable(1);

    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        final RedditPost post = RedditPost.fromJSON(value.toString().split("\t+")[1]);
        final String subreddit = post.getSubreddit().toString();

        final DateTime postCreationTime = new DateTime(post.getCreated_utc().get() * 1000L, DateTimeZone.UTC);
        if (postCreationTime.isAfter(oneWeekAgo)) {
            context.write(post.getSubreddit(), one);
        }
    }
}
