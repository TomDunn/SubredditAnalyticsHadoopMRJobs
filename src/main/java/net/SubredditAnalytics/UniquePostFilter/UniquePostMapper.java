package net.SubredditAnalytics.UniquePostFilter;

import net.SubredditAnalytics.Model.RedditPost;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created by tom on 8/30/14.
 */
public class UniquePostMapper extends Mapper<LongWritable, Text, Text, RedditPost> {
    private Text postName = new Text();

    protected void map(LongWritable key, Text value, Context context) throws InterruptedException, IOException {
        RedditPost post = RedditPost.fromJSON(value.toString());
        final String name = post.getName().toString();

        if (name.length() == 0) {
            return;
        }

        postName.set(name);
        context.write(this.postName, post);
    }
}
