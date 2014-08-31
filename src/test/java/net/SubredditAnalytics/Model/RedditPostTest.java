package net.SubredditAnalytics.Model;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * Created by tom on 8/31/14.
 */
public class RedditPostTest {
    private final String postId1 = "2euwo8";
    private final String postId2 = "3axxx9";

    private final long last_seen1 = 1409389198L;
    private final long last_seen2 = 1409389200L;

    private RedditPost p1;
    private RedditPost p2;

    @Before
    public void makePosts() {
        p1 = new RedditPost();
        p2 = new RedditPost();
    }

    @Test
    public void testCompareTo() {
        Assert.assertEquals("empty RedditPosts should have compareTo of 0", 0, p1.compareTo(p2));

        p1.setId(new Text(postId1));

        Assert.assertNotEquals("empty and non-empty should not compareTo", 0, p1.compareTo(p2));

        p1.setLast_seen(new LongWritable(last_seen1));
        p2.setId(new Text(postId1));
        p2.setLast_seen(new LongWritable(last_seen2));

        Assert.assertNotEquals("same id but different last_seen should result in non-zero compareTo", 0, p1.compareTo(p2));

        p1.setLast_seen(new LongWritable(last_seen2));

        Assert.assertEquals("same id and same last_seen should have compareTo of 0", 0, p1.compareTo(p2));
    }

    @Test
    public void hashCodeTest() {
        Assert.assertEquals("hashcodes of empty should be equal", p1.hashCode(), p2.hashCode());

        p1.setId(new Text(postId1));
        p2.setId(new Text(postId2));

        Assert.assertNotEquals("posts w/o same id should have different hashcodes", p1.hashCode(), p2.hashCode());
        p2.setId(new Text(postId1));
        Assert.assertEquals("posts w/ same id should have common hashcode", p1.hashCode(), p2.hashCode());

        p1.setLast_seen(new LongWritable(last_seen1));
        p2.setLast_seen(new LongWritable(last_seen2));

        Assert.assertNotEquals(p1.hashCode(), p2.hashCode());

        p2.setLast_seen(new LongWritable(last_seen1));
        Assert.assertEquals(p1.hashCode(), p2.hashCode());
    }
}
