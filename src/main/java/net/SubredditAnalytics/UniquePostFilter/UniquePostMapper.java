package net.SubredditAnalytics.UniquePostFilter;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import java.io.IOException;

/**
 * Created by tom on 8/30/14.
 */
public class UniquePostMapper extends Mapper<LongWritable, Text, Text, Text> {
    private final JSONParser parser = new JSONParser();
    private Text postName = new Text();
    private Text postJSON = new Text();

    protected void map(LongWritable key, Text value, Context context) throws InterruptedException, IOException {
        String[] columns = value.toString().split("\t+");

        try {
            final JSONObject redditSubmissionJSON = (JSONObject) parser.parse(columns[2]);
            final String name = (String) redditSubmissionJSON.get("name");

            postName.set(name);
            postJSON.set(columns[2]);

            context.write(this.postName, this.postJSON);
        } catch (ParseException e) {
            return;
        }
    }
}
