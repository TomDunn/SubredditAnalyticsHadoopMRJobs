package net.SubredditAnalytics.SubredditLinkDomainCount;

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
    private final JSONParser parser = new JSONParser();
    private final static IntWritable count = new IntWritable(1);
    private Text subredditDomainKey = new Text();

    protected void map(Text key, Text value, Context context) throws InterruptedException, IOException {

        try {
            final JSONObject postJSON = (JSONObject) parser.parse(value.toString());
            final String subreddit = (String) postJSON.get("subreddit");
            final String domain    = (String) postJSON.get("domain");

            subredditDomainKey.set(subreddit + "|" + domain);
            context.write(subredditDomainKey, count);
        } catch (ParseException parseException) {
            return;
        }
    }
}
