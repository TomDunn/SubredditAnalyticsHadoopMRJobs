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

    MapDriver<LongWritable, Text, Text, IntWritable> mapDriver;
    private final static String jsonLine = "LOGCOL1\tLOGCOL2\t{\"subreddit\":\"funny\",\"created_utc\":1409259817.0}";

    @Before
    public void setUp() {
        HourlyPostCountMapper mapper = new HourlyPostCountMapper();
        mapDriver = MapDriver.newMapDriver();
        mapDriver.setMapper(mapper);
    }

    @Test
    public void testMapper() throws IOException {
        mapDriver.withInput(new LongWritable(1), new Text(jsonLine));
        mapDriver.withOutput(new Text("funny|1409259600"), new IntWritable(1));
        mapDriver.runTest();
    }

    @Test
    public void testMapperKeyMaker() {
        final HourlyPostCountMapper mapper = new HourlyPostCountMapper();

        // unix timestamp
        final Number TS         = 1409426740L;
        final String subreddit  = "funny";

        final String key = mapper.makeSubredditHourlyKey(subreddit, TS);
        Assert.assertEquals("Key should equal funny|1409425200", "funny|1409425200", key);
    }
}
