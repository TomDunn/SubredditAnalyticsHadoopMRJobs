package net.SubredditAnalytics.SubmissionXPosts;

import net.SubredditAnalytics.Model.RedditPost;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by tom on 9/28/14.
 */
public class SubmissionXPostMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    private final static Pattern xPostPattern = Pattern.compile("r/[a-zA-Z0-9_]+");
    private final static IntWritable countOne = new IntWritable(1);

    public void map(LongWritable key, Text inputText, Context context) throws IOException, InterruptedException {
        RedditPost redditPost = RedditPost.fromJSON(inputText.toString().split("\t+")[1]);
        final String postTitle = redditPost.getTitle().toString().toLowerCase();

        for (final String xPostedSub : getXPostedSubreddits(postTitle)) {
            context.write(new Text(redditPost.getSubreddit().toString().toLowerCase() + " -> " + xPostedSub), countOne);
        }
    }

    public List<String> getXPostedSubreddits(final String title) {
        List<String> otherSubreddits = new ArrayList<String>();
        Matcher subredditMatcher = xPostPattern.matcher(title);

        while (subredditMatcher.find()) {
            otherSubreddits.add(subredditMatcher.group().split("/")[1]);
        }

        return otherSubreddits;
    }
}
