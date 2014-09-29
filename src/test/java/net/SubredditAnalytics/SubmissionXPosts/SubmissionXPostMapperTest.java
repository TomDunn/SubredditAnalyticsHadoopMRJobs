package net.SubredditAnalytics.SubmissionXPosts;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

/**
 * Created by tom on 9/28/14.
 */
public class SubmissionXPostMapperTest {
    final String testPostJSON = "ID\t{\"likes\": null, \"stickied\": false, \"secure_media\": null, \"ups\": 12, \"gilded\": 0, \"url\": \"http://www.reddit.com/r/duolingo/comments/2gds84/whats_happening_with_the_polish_course/\", \"over_18\": false, \"title\": \"What's happening with the Polish [Duolingo] course? (x-post r/duolingo)\", \"author_flair_css_class\": null, \"banned_by\": null, \"domain\": \"reddit.com\", \"thumbnail\": \"default\", \"subreddit\": \"learnpolish\", \"author_flair_text\": null, \"subreddit_id\": \"t5_2u72v\", \"id\": \"2ggazg\", \"last_seen\": 1411025252, \"num_comments\": 1, \"is_self\": false, \"score\": 12, \"link_flair_text\": null, \"author\": \"gk3coloursred\", \"num_reports\": null, \"edited\": false, \"hidden\": false, \"link_flair_css_class\": null, \"name\": \"t3_2ggazg\", \"media\": null, \"selftext\": \"\", \"distinguished\": null, \"approved_by\": null, \"report_reasons\": null, \"downs\": 0, \"created_utc\": 1410783226.0}";
    private MapDriver<LongWritable, Text, Text, IntWritable> mapDriver;

    @Before
    public void setUp() {
        mapDriver = new MapDriver<LongWritable, Text, Text, IntWritable>();
        mapDriver.setMapper(new SubmissionXPostMapper());
    }

    @Test
    public void testMapper() throws IOException {
        mapDriver.withInput(new LongWritable(1), new Text(testPostJSON));
        mapDriver.withOutput(new Text("learnpolish -> duolingo"), new IntWritable(1));
        mapDriver.runTest();
    }

    @Test
    public void testXPostFinder() {
        SubmissionXPostMapper mapper = new SubmissionXPostMapper();

        final String testTitle1 = "(x-post from /r/hearthstone)";
        Assert.assertEquals("hearthstone", mapper.getXPostedSubreddits(testTitle1).get(0));

        final String testTitle2 = "[x-post from /r/Tumblr] The Sims get bleak";
        Assert.assertEquals("Tumblr", mapper.getXPostedSubreddits(testTitle2).get(0));
    }
}
