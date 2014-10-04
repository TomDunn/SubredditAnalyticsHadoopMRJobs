package net.SubredditAnalytics.SubredditsActivePastWeek;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by tom on 10/4/14.
 */
public class SubredditsActivePastWeekReducerTest {
    private ReduceDriver<Text, IntWritable, Text, IntWritable> reduceDriver = new ReduceDriver<Text, IntWritable, Text, IntWritable>();

    /* test data */
    private final IntWritable one = new IntWritable(1);
    private List<IntWritable> counts;
    private final String key = "funny";

    @Before
    public void setUp() {
        counts = new ArrayList<IntWritable>(1);
        counts.add(one);
        counts.add(one);

        reduceDriver.setReducer(new SubredditsActivePastWeekReducer());
    }

    @Test
    public void testReducer() throws IOException {
        reduceDriver.withInput(new Text(key), counts);
        reduceDriver.withOutput(new Text(key), new IntWritable(2));
        reduceDriver.runTest();
    }
}
