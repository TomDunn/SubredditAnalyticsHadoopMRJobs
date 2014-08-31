package net.SubredditAnalytics.HourlyPostCount;

import net.SubredditAnalytics.Model.RedditPost;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;

import java.math.BigDecimal;

/**
 * Created by tom on 8/29/14.
 */
public class HourlyPostCountMapper extends Mapper<LongWritable,Text,Text,IntWritable> {
    private final static IntWritable count = new IntWritable(1);
    private Text time = new Text();

    private final JSONParser parser = new JSONParser();

    protected void map(LongWritable key, Text value, Context context) throws java.io.IOException, InterruptedException {
        String[] columns = value.toString().split("\t+");

        final RedditPost post = RedditPost.fromJSON(columns[1]);
        final long createdUTC = post.getCreated_utc().get();
        final String subreddit = post.getSubreddit().toString();

        time.set(this.makeSubredditHourlyKey(subreddit, createdUTC));

        context.write(time, count);
    }

    /* package private */ String makeSubredditHourlyKey(final String subredditName, final Number createdUTC) {
        final DateTime createdUTCDateTime = new DateTime(createdUTC.longValue() * 1000L, DateTimeZone.UTC);
        final DateTime hour = createdUTCDateTime.hourOfDay().roundFloorCopy();
        final long hourTS = hour.getMillis() / 1000L;
        return subredditName + "|" + Long.toString(hourTS);
    }
}
