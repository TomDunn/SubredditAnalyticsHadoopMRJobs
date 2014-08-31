package net.SubredditAnalytics.UniquePostFilter;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.apache.hadoop.mrunit.types.Pair;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * Created by tom on 8/30/14.
 */
public class UniquePostReducerTest {
    private ReduceDriver<Text, Text, Text, Text> reduceDriver;

    private final String post1_id = "t3_2euwng";
    private final String post1_V1 = "{\"name\":\"t3_2euwng\",\"last_seen\":1409389198}";
    private final String post1_V2 = "{\"name\":\"t3_2euwng\",\"last_seen\":1409389200}";
    private final String post1_V3 = "{\"name\":\"t3_2euwng\",\"last_seen\":1409389210}";
    private final Pair<Text, Text> post1_expected = new Pair<Text, Text>(new Text(post1_id), new Text(post1_V3));

    private final String post2_id = "t3_ABC";
    private final String post2_V1 = "{\"name\":\"t3_ABC\",\"last_seen\":1409389198}";
    private final String post2_V2 = "{\"name\":\"t3_ABC\",\"last_seen\":1409389200}";
    private final Pair<Text, Text> post2_expected = new Pair<Text,Text>(new Text(post2_id), new Text(post2_V2));

    @Before
    public void setUp() {
        reduceDriver = new ReduceDriver<Text, Text, Text, Text>();
        UniquePostReducer reducer = new UniquePostReducer();
        reduceDriver.setReducer(reducer);
    }

    @Test
    public void testReducer() throws IOException {
        List<Text> post1Values = new ArrayList<Text>();
        List<Text> post2Values = new ArrayList<Text>();

        post1Values.add(new Text(post1_V1));
        post1Values.add(new Text(post1_V2));
        post1Values.add(new Text(post1_V3));

        post2Values.add(new Text(post2_V1));
        post2Values.add(new Text(post2_V2));

        reduceDriver.withInput(new Text(post1_id), post1Values);
        reduceDriver.withInput(new Text(post2_id), post2Values);

        reduceDriver.withOutput(new Text(post1_id), new Text(post1_V3));
        reduceDriver.withOutput(new Text(post2_id), new Text(post2_V2));

        reduceDriver.runTest();
    }
}
