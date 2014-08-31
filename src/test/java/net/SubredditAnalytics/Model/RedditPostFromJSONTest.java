package net.SubredditAnalytics.Model;

import net.SubredditAnalytics.Model.RedditPost;
import static org.junit.Assert.*;
import org.junit.Before;
import org.junit.Test;

/**
 * Created by tom on 8/31/14.
 */
public class RedditPostFromJSONTest {
    // ok input
    final String goodJSON = "{\"domain\":\"self.Python\",\"subreddit\":\"Python\",\"selftext\":\"I like Python\",\"id\":\"2eulrh\",\"author\":\"rhgrant10\",\"score\":15,\"over_18\":false,\"thumbnail\":\"\",\"subreddit_id\":\"t5_2qh0y\",\"downs\":0,\"is_self\":true,\"name\":\"t3_2eulrh\",\"url\":\"http://google.com\",\"title\":\"First job\",\"created_utc\":1409254019.0,\"ups\":15,\"last_seen\":1409388890}";

    // missing over_18
    final String missingBoolean = "{\"domain\":\"self.Python\",\"subreddit\":\"Python\",\"selftext\":\"I like Python\",\"id\":\"2eulrh\",\"author\":\"rhgrant10\",\"score\":15,\"thumbnail\":\"\",\"subreddit_id\":\"t5_2qh0y\",\"downs\":0,\"name\":\"t3_2eulrh\",\"url\":\"http://google.com\",\"title\":\"First job\",\"created_utc\":1409254019.0,\"ups\":15,\"last_seen\":1409388890}";

    // missing domain, subreddit, selftext, id, author, thumbnail, subreddit_id, name, url ,title
    final String missingFields = "{\"score\":15,\"over_18\":false,\"downs\":0,\"is_self\":true,\"created_utc\":1409254019.0,\"ups\":15,\"last_seen\":1409388890}";

    // missing numeric fields; score, downs, ups, created_utc
    final String missingNumericFields = "{\"domain\":\"self.Python\",\"subreddit\":\"Python\",\"selftext\":\"I like Python\",\"id\":\"2eulrh\",\"over_18\":false,\"is_self\":true}";

    RedditPost post;

    @Before
    public void setUp() {
        post = new RedditPost();
    }

    @Test
    public void happyCase() {
        post.loadFromJSONString(goodJSON);

        assertEquals("domain should be set", "self.Python", post.getDomain().toString());
        assertEquals("subreddit should be set", "Python", post.getSubreddit().toString());
        assertEquals("self text should be set", "I like Python", post.getSelftext().toString());
        assertEquals("id should be set", "2eulrh", post.getId().toString());
        assertEquals("author should be set", "rhgrant10", post.getAuthor().toString());
        assertEquals("score should be set", 15, post.getScore().get());
        assertEquals("over 18 should be false", false, post.getOver_18().get());
        assertEquals("thumbnail should be blank", "", post.getThumbnail().toString());
        assertEquals("subreddit id", "t5_2qh0y", post.getSubreddit_id().toString());
        assertEquals("down votes is 0", 0, post.getDowns().get());
        assertEquals("is_self should be true", true, post.getIs_self().get());
        assertEquals("name is set", "t3_2eulrh", post.getName().toString());
        assertEquals("url is set", "http://google.com", post.getUrl().toString());
        assertEquals("title is set", "First job", post.getTitle().toString());
        assertEquals("created utc is set", 1409254019L, post.getCreated_utc().get());
        assertEquals("ups is set", 15, post.getUps().get());
        assertEquals("last seen is set", 1409388890L, post.getLast_seen().get());
    }

    @Test
    public void missingOver18() {
        post.loadFromJSONString(missingBoolean);

        assertEquals("over 18 should be defaulted to false", false, post.getOver_18().get());
        assertEquals("is_self should be false", false, post.getIs_self().get());
    }

    @Test
    public void missingStringFields() {
        post.loadFromJSONString(missingFields);

        assertEquals("domain should be empty", "", post.getDomain().toString());
        assertEquals("subreddit should be empty", "", post.getSubreddit().toString());
        assertEquals("self text should be empty", "", post.getSelftext().toString());
        assertEquals("id should be emtpy", "", post.getId().toString());
        assertEquals("author should be empty", "", post.getAuthor().toString());
        assertEquals("thumbnail should be blank", "", post.getThumbnail().toString());
        assertEquals("subreddit id should be empty", "", post.getSubreddit_id().toString());
        assertEquals("name should be empty", "", post.getName().toString());
        assertEquals("url is empty", "", post.getUrl().toString());
        assertEquals("title is empty", "", post.getTitle().toString());
    }

    @Test
    public void missingNumericFields() {
        post.loadFromJSONString(missingNumericFields);

        assertEquals("score should be 0", 0, post.getScore().get());
        assertEquals("down votes is 0", 0, post.getDowns().get());
        assertEquals("created utc is 0", 0L, post.getCreated_utc().get());
        assertEquals("ups is 0", 0, post.getUps().get());
        assertEquals("last seen is 0", 0L, post.getLast_seen().get());
    }
}
