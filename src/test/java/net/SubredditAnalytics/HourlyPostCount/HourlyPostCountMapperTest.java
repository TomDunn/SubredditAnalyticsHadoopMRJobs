package net.SubredditAnalytics.HourlyPostCount;

import net.SubredditAnalytics.HourlyPostCount.HourlyPostCountMapper;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

/**
 * Created by tom on 8/30/14.
 */
public class HourlyPostCountMapperTest {

    MapDriver<LongWritable, Text, Text, Text> mapDriver;
    private final static String jsonLine = "POST_ID\t{\"subreddit\":\"funny\",\"created_utc\":1410122835.0}";

    @Before
    public void setUp() {
        HourlyPostCountMapper mapper = new HourlyPostCountMapper();
        mapDriver = MapDriver.newMapDriver();
        mapDriver.setMapper(mapper);
    }

    @Test
    public void testMapper() throws IOException {
        mapDriver.withInput(new LongWritable(1), new Text(jsonLine));
        mapDriver.withOutput(new Text("funny|1410048000"), new Text("1410120000|1"));
        mapDriver.runTest();
    }

    @Test
    public void testMakeSubredditDailyTimestampKey() {
        final HourlyPostCountMapper mapper = new HourlyPostCountMapper();

        // unix timestamp
        final Number TS         = 1409426740L;
        final String subreddit  = "funny";

        final String key = mapper.makeSubredditDailyTimestampKey(subreddit, TS);
        Assert.assertEquals("Key should equal funny|1409356800", "funny|1409356800", key);
    }

    @Test
    public void testGetDayTS() {
        final Long timestamp = 1410122835L;
        final Long expectedTimestamp = 1410048000L;

        final HourlyPostCountMapper mapper = new HourlyPostCountMapper();

        Assert.assertEquals("Timestamp should equal Sept 7th 2014 at 00:00 UTC", expectedTimestamp, mapper.getDayTimestamp(1410122835));
    }
}
