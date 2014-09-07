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
public class HourlyPostCountMapper extends Mapper<LongWritable,Text,Text,Text> {
    private Text subredditDayKey = new Text();
    private Text hourCountKey    = new Text();

    private final JSONParser parser = new JSONParser();

    protected void map(LongWritable key, Text value, Context context) throws java.io.IOException, InterruptedException {
        String[] columns = value.toString().split("\t+");

        final RedditPost post = RedditPost.fromJSON(columns[1]);
        final long createdUTC = post.getCreated_utc().get();
        final String subreddit = post.getSubreddit().toString();

        subredditDayKey.set(this.makeSubredditDailyTimestampKey(subreddit, createdUTC));
        hourCountKey.set(this.makeHourCountKey(createdUTC));

        context.write(subredditDayKey, hourCountKey);
    }

    /* package private */ String makeHourCountKey(final Number createdUTC) {
        final Long hourTS = this.getHourTimestamp(createdUTC);
        return Long.toString(hourTS) + "|1";
    }

    /* package private */ Long getDayTimestamp(final Number createdUTC) {
        final DateTime createdUTCDateTime = new DateTime(createdUTC.longValue() * 1000L, DateTimeZone.UTC);
        final Long dayTS = createdUTCDateTime.getMillis() -  createdUTCDateTime.getMillisOfDay();
        return dayTS / 1000L;
    }

    /* package private */ Long getHourTimestamp(final Number createdUTC) {
        final DateTime createdUTCDateTime = new DateTime(createdUTC.longValue() * 1000L, DateTimeZone.UTC);
        final DateTime hour = createdUTCDateTime.hourOfDay().roundFloorCopy();
        final long hourTS = hour.getMillis() / 1000L;
        return hourTS;
    }

    /* package private */ String makeSubredditDailyTimestampKey(final String subredditName, final Number createdUTC) {
        final Long ts = this.getDayTimestamp(createdUTC);
        return subredditName + "|" + Long.toString(ts);
    }
}
