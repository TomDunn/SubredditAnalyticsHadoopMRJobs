package net.SubredditAnalytics.HourlyPostCount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created by tom on 8/30/14.
 */
public class HourlyPostCountReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
    private IntWritable totalCount = new IntWritable();

    protected void reduce(Text key, Iterable<IntWritable> counts, Context context) throws IOException, InterruptedException {
        int total = 0;

        for (IntWritable count : counts) {
            total += count.get();
        }

        totalCount.set(total);
        context.write(key, totalCount);
    }
}
