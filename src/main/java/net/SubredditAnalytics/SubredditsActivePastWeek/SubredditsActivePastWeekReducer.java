package net.SubredditAnalytics.SubredditsActivePastWeek;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created by tom on 10/4/14.
 */
public class SubredditsActivePastWeekReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

    public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int total = 0;
        for (IntWritable value : values) {
            total += value.get();
        }

        context.write(key, new IntWritable(total));
    }
}
