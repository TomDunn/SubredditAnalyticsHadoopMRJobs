package net.SubredditAnalytics.UniquePostFilter;

import net.SubredditAnalytics.Model.RedditPost;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

/**
 * Created by tom on 8/30/14.
 */
public class UniquePostMapperTest {
    private final static String inputLine = "2014-08-30T08:59:58Z\tra.submissions.post\t{\"name\":\"t3_2euwng\",\"last_seen\":1409389198}";
    MapDriver<LongWritable, Text, Text, RedditPost> mapDriver;

    @Before
    public void setUp() {
        mapDriver = new MapDriver<LongWritable, Text, Text, RedditPost>();
        UniquePostMapper mapper = new UniquePostMapper();
        mapDriver.setMapper(mapper);
    }

    @Test
    public void mapperTest() throws IOException {
        mapDriver.withInput(new LongWritable(1), new Text(inputLine));

        RedditPost post = new RedditPost();
        post.loadFromJSONString(inputLine.split("\t+")[2]);

        mapDriver.withOutput(new Text("t3_2euwng"), post);
        mapDriver.runTest();
    }
}
