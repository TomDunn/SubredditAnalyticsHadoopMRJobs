package net.SubredditAnalytics.SubredditLinkDomainCount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by tom on 8/31/14.
 */
public class SubredditLinkDomainCountReducerTest {
    private ReduceDriver<Text, IntWritable, Text, IntWritable> reduceDriver;

    /* Test data */
    private final Text key1 = new Text("all|imgur.com");
    private final Text key2 = new Text("Python|imgur.com");
    private final Text key3 = new Text("AskReddit|google.com");
    private final IntWritable one = new IntWritable(1);

    @Before
    public void setUp() {
        SubredditLinkDomainCountReducer reducer = new SubredditLinkDomainCountReducer();
        reduceDriver = new ReduceDriver<Text, IntWritable, Text, IntWritable>();
        reduceDriver.setReducer(reducer);
    }

    @Test
    public void testReducer() throws IOException {
        List<IntWritable> key1Values = new ArrayList<IntWritable>();
        key1Values.add(one);
        key1Values.add(one);

        List<IntWritable> key2Values = new ArrayList<IntWritable>();
        key2Values.add(one);

        List<IntWritable> key3Values = new ArrayList<IntWritable>();
        key3Values.add(one);
        key3Values.add(one);

        reduceDriver.withInput(key1, key1Values);
        reduceDriver.withInput(key2, key2Values);
        reduceDriver.withInput(key3, key3Values);

        reduceDriver.withOutput(key1, new IntWritable(2));
        reduceDriver.withOutput(key2, new IntWritable(1));
        reduceDriver.withOutput(key3, new IntWritable(2));

        reduceDriver.runTest();
    }
}
