package net.SubredditAnalytics.UniquePostFilter;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import java.io.IOException;

/**
 * Created by tom on 8/30/14.
 */
public class UniquePostReducer extends Reducer<Text, Text, Text, Text> {
    private final JSONParser parser = new JSONParser();

    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        long maxLastSeen = 0L;
        Text latestPostJson = new Text();

        for (Text post : values) {
            try {
                final JSONObject postJson = (JSONObject) parser.parse(post.toString());
                final long lastSeen = ((Number) postJson.get("last_seen")).longValue();

                if (lastSeen > maxLastSeen) {
                    maxLastSeen = lastSeen;
                    latestPostJson.set(post);
                }
            } catch (ParseException parseException) {
                continue;
            }
        }

        context.write(key, latestPostJson);
    }
}
