package net.SubredditAnalytics.SubmissionXPosts;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created by tom on 9/28/14.
 */
public class SubmissionXPostReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

    public void reduce(Text key, Iterable<IntWritable> counts, Context context) throws InterruptedException, IOException {
        int total = 0;

        for (IntWritable count : counts) {
            total += count.get();
        }

        context.write(key, new IntWritable(total));
    }
}
