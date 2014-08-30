package net.SubredditAnalytics;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by tom on 8/30/14.
 */
public class HourlyPostCountReducerTest {
    ReduceDriver reduceDriver;

    @Before
    public void setUp() {
        HourlyPostCountReducer reducer = new HourlyPostCountReducer();
        reduceDriver = new ReduceDriver<Text, IntWritable, Text, IntWritable>();
        reduceDriver.setReducer(reducer);
    }

    @Test
    public void testHourlyReducer() throws IOException{
        List counts = new ArrayList<IntWritable>();
        counts.add(new IntWritable(1));
        counts.add(new IntWritable(2));
        counts.add(new IntWritable(3));

        final Text key = new Text("funny|123123123");
        reduceDriver.withInput(key, counts);
        reduceDriver.withOutput(key, new IntWritable(5));
        reduceDriver.runTest();
    }
}
