package net.SubredditAnalytics.TopDomains;

import com.google.common.base.Joiner;
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
public class TopDomainReducerTest {
    private ReduceDriver<Text,Text,Text,Text> reduceDriver;

    /* test data */
    private final String domainCount1 = "google.com|2";
    private final String domainCount2 = "imgur.com|2";

    private final String[] domainCounts = {domainCount1, domainCount2};
    private final String[] singleDomainCount = {domainCount1};

    private final String combined1 = Joiner.on(",").join(domainCounts);
    private final String combined2 = Joiner.on(",").join(singleDomainCount);

    private final Text key = new Text("Python");

    @Before
    public void setUp() {
        TopDomainReducer reducer = new TopDomainReducer();
        reduceDriver = new ReduceDriver<Text, Text, Text, Text>();
        reduceDriver.setReducer(reducer);
    }

    @Test
    public void testReducer() throws IOException {
        List<Text> inputs = new ArrayList<Text>();
        inputs.add(new Text(combined1));
        inputs.add(new Text(combined2));

        reduceDriver.withInput(key, inputs);
        reduceDriver.withOutput(key, new Text("google.com|4,imgur.com|2"));

        reduceDriver.runTest();
    }
}
