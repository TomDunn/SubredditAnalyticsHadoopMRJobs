package net.SubredditAnalytics.Parser;

import net.SubredditAnalytics.Model.RedditPost;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

/**
 * Created by tom on 8/31/14.
 */
public class RedditPostFromJSONFactory {
    private final JSONParser parser = new JSONParser();

    public RedditPost fromJSON(final String json) {
        RedditPost redditPost = new RedditPost();

        try {
            final JSONObject postJSON = (JSONObject) parser.parse(json);

            final String domain = (String) postJSON.get("domain");
            redditPost.setDomain(new Text(domain == null ? "" : domain));

            final String subreddit = (String) postJSON.get("subreddit");
            redditPost.setSubreddit(new Text(subreddit == null ? "" : subreddit));

            final String selftext = (String) postJSON.get("selftext");
            redditPost.setSelftext(new Text(selftext == null ? "" : selftext));

            final String id = (String) postJSON.get("id");
            redditPost.setId(new Text(id == null ? "" : id));

            final String author = (String) postJSON.get("author");
            redditPost.setAuthor(new Text(author == null ? "" : author));

            final Number score = (Number) postJSON.get("score");
            redditPost.setScore(new IntWritable(score == null ? 0 : score.intValue()));

            final Boolean over_18 = (Boolean) postJSON.get("over_18");
            redditPost.setOver_18(new BooleanWritable(over_18 == null ? false : over_18));

            final String thumbnail = (String) postJSON.get("thumbnail");
            redditPost.setThumbnail(new Text(thumbnail == null ? "" : thumbnail));

            final String subreddit_id = (String) postJSON.get("subreddit_id");
            redditPost.setSubreddit_id(new Text(subreddit_id == null ? "" : subreddit_id));

            final Number downs = (Number) postJSON.get("downs");
            redditPost.setDowns(new IntWritable(downs == null ? 0 : downs.intValue()));

            final Boolean is_self = (Boolean) postJSON.get("is_self");
            redditPost.setIs_self(new BooleanWritable(is_self == null ? false : is_self));

            final String name = (String) postJSON.get("name");
            redditPost.setName(new Text(name == null ? "" : name));

            final String url = (String) postJSON.get("url");
            redditPost.setUrl(new Text(url == null ? "" : url));

            final String title = (String) postJSON.get("title");
            redditPost.setTitle(new Text(title == null ? "" : title));

            final Number created_utc = (Number) postJSON.get("created_utc");
            redditPost.setCreated_utc(new LongWritable(created_utc == null ? 0L : created_utc.longValue()));

            final Number ups = (Number) postJSON.get("ups");
            redditPost.setUps(new IntWritable(ups == null ? 0 : ups.intValue()));

            final Number last_seen = (Number) postJSON.get("last_seen");
            redditPost.setLast_seen(new LongWritable(last_seen == null ? 0 : last_seen.longValue()));
        } catch (ParseException parseException) {
            // silently
        }

        return redditPost;
    }
}
