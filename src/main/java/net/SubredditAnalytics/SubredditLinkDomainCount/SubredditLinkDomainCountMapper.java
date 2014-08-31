package net.SubredditAnalytics.SubredditLinkDomainCount;

import net.SubredditAnalytics.Model.RedditPost;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import java.io.IOException;

/**
 * Created by tom on 8/30/14.
 */
public class SubredditLinkDomainCountMapper extends Mapper<Text, Text, Text, IntWritable> {
    private final static IntWritable count = new IntWritable(1);
    private Text subredditDomainKey = new Text();

    protected void map(Text key, Text value, Context context) throws InterruptedException, IOException {
        final RedditPost post = RedditPost.fromJSON(value.toString());
        final String subreddit = post.getSubreddit().toString();
        final String domain = post.getDomain().toString();

        subredditDomainKey.set(subreddit + "|" + domain);
        context.write(subredditDomainKey, count);
        subredditDomainKey.set("all" + "|" + domain);
        context.write(subredditDomainKey, count);
    }
}
