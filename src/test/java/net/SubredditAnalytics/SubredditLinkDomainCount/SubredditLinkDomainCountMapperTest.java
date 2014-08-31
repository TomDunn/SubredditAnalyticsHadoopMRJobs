package net.SubredditAnalytics.SubredditLinkDomainCount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

/**
 * Created by tom on 8/30/14.
 */
public class SubredditLinkDomainCountMapperTest {
    private MapDriver<LongWritable, Text, Text, IntWritable> mapDriver;

    /* Test data */
    final String t1 = "ID\t{\"domain\":\"imgur.com\",\"subreddit\":\"funny\"}";
    final Text key = new Text("");

    @Before
    public void setUp() {
        mapDriver = new MapDriver<LongWritable, Text, Text, IntWritable>();
        mapDriver.setMapper(new SubredditLinkDomainCountMapper());
    }

    @Test
    public void mapperTest() throws IOException {
        mapDriver.withInput(new LongWritable(1), new Text(t1));
        mapDriver.withOutput(new Text("funny|imgur.com"), new IntWritable(1));
        mapDriver.withOutput(new Text("all|imgur.com"), new IntWritable(1));
        mapDriver.runTest();
    }
}
