package net.SubredditAnalytics.SubmissionXPosts;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by tom on 9/28/14.
 */
public class SubmissionXPostReducerTest {
    private ReduceDriver<Text, IntWritable, Text, IntWritable> reduceDriver;
    private final IntWritable one = new IntWritable(1);

    @Before
    public void setUp() {
        reduceDriver = new ReduceDriver<Text, IntWritable, Text, IntWritable>();
        reduceDriver.setReducer(new SubmissionXPostReducer());
    }

    @Test
    public void testReducer() throws IOException {
        final String key1 = "learnpolish -> duolingo";
        List<IntWritable> values = new ArrayList<IntWritable>();

        values.add(one);
        values.add(one);

        reduceDriver.withInput(new Text(key1), values);
        reduceDriver.withOutput(new Text(key1), new IntWritable(2));

        reduceDriver.runTest();
    }
}
