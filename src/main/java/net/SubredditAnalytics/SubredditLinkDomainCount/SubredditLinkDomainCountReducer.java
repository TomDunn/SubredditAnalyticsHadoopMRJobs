package net.SubredditAnalytics.SubredditLinkDomainCount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created by tom on 8/31/14.
 */
public class SubredditLinkDomainCountReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

    public void reduce(Text key, Iterable<IntWritable> counts, Context context) throws InterruptedException, IOException {
        int totalCount = 0;

        for (IntWritable count : counts) {
            totalCount += count.get();
        }

        context.write(key, new IntWritable(totalCount));
    }
}
