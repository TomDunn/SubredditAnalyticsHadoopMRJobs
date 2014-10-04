package net.SubredditAnalytics.SubredditsActivePastWeek;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

/**
 * Created by tom on 10/4/14.
 */
public class SubredditsActivePastWeekMapperTest {
    private MapDriver<LongWritable, Text, Text, IntWritable> mapDriver = new MapDriver<LongWritable, Text, Text, IntWritable>();

    final String t1 = "ID\t{\"domain\":\"imgur.com\",\"subreddit\":\"funny\", \"created_utc\": CREATED_TIME_UTC}";
    final String t2 = "ID\t{\"domain\":\"imgur.com\",\"subreddit\":\"notfunny\", \"created_utc\": CREATED_TIME_UTC}";


    @Before
    public void setUp() {
        mapDriver.setMapper(new SubredditsActivePastWeekMapper());
    }

    @Test
    public void testMapper() throws IOException {
        DateTime now = DateTime.now(DateTimeZone.UTC);

        String postWithinPastWeek = t1.replace("CREATED_TIME_UTC", Long.toString(now.minusDays(6).getMillis() / 1000L));
        String oldPost = t2.replace("CREATED_TIME_UTC", Long.toString(0));

        mapDriver.withInput(new LongWritable(1), new Text(postWithinPastWeek));
        mapDriver.withInput(new LongWritable(2), new Text(oldPost));

        mapDriver.withOutput(new Text("funny"), new IntWritable(1));
        mapDriver.runTest();
    }
}
