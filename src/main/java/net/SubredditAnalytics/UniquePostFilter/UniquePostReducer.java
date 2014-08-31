package net.SubredditAnalytics.UniquePostFilter;

import net.SubredditAnalytics.Model.RedditPost;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import java.io.IOException;

/**
 * Created by tom on 8/30/14.
 */
public class UniquePostReducer extends Reducer<Text, RedditPost, Text, RedditPost> {

    protected void reduce(Text key, Iterable<RedditPost> posts, Context context) throws IOException, InterruptedException {
        long maxLastSeen = 0L;
        RedditPost latestPost = new RedditPost();

        for (RedditPost post : posts) {
            final long lastSeen = post.getLast_seen().get();

            if (lastSeen > maxLastSeen) {
                maxLastSeen = lastSeen;
                latestPost = post;
            }
        }

        context.write(key, latestPost);
    }
}
