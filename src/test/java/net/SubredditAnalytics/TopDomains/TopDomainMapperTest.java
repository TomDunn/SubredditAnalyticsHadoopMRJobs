package net.SubredditAnalytics.TopDomains;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

/**
 * Created by tom on 8/31/14.
 */
public class TopDomainMapperTest {
    private MapDriver<LongWritable, Text, Text, Text> mapDriver;

    /* Test data */
    private final String test1 = "Python|imgur.com\t2";
    private final String test2 = "AskReddit|google.com\t3";

    @Before
    public void setUp() {
        TopDomainMapper mapper = new TopDomainMapper();
        mapDriver = new MapDriver<LongWritable, Text, Text, Text>();
        mapDriver.setMapper(mapper);
    }

    @Test
    public void testMapper() throws IOException {
        mapDriver.withInput(new LongWritable(), new Text(test1));
        mapDriver.withInput(new LongWritable(), new Text(test2));

        mapDriver.withOutput(new Text("Python"), new Text("imgur.com|2"));
        mapDriver.withOutput(new Text("Python"), new Text("_TOTAL_|2"));
        mapDriver.withOutput(new Text("AskReddit"), new Text("google.com|3"));
        mapDriver.withOutput(new Text("AskReddit"), new Text("_TOTAL_|3"));

        mapDriver.runTest();
    }
}
