package net.SubredditAnalytics.HourlyPostCount;

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
    ReduceDriver<Text,Text,Text,Text> reduceDriver;

    @Before
    public void setUp() {
        HourlyPostCountReducer reducer = new HourlyPostCountReducer();
        reduceDriver = new ReduceDriver<Text, Text, Text, Text>();
        reduceDriver.setReducer(reducer);
    }

    @Test
    public void testHourlyReducer() throws IOException{
        List<Text> counts = new ArrayList<Text>();
        counts.add(new Text("123|1"));
        counts.add(new Text("123|1"));
        counts.add(new Text("124|1"));
        counts.add(new Text("125|1,125|1"));

        final Text key = new Text("funny|123123123");
        reduceDriver.withInput(key, counts);

        // This may break at any time, no ordering guarantee on hash sets/maps
        // TODO make the reducer sort by hour.
        reduceDriver.withOutput(key, new Text("125|2,124|1,123|2"));
        reduceDriver.runTest();
    }
}
