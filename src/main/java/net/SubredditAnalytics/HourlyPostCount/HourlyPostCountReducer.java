package net.SubredditAnalytics.HourlyPostCount;

import com.google.common.base.Joiner;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Created by tom on 8/30/14.
 */
public class HourlyPostCountReducer extends Reducer<Text, Text, Text, Text> {
    protected void reduce(Text key, Iterable<Text> hourCountGroups, Context context) throws IOException, InterruptedException {
        int total = 0;
        Map<String,Integer> hourCountMap = new HashMap<String, Integer>();

        for (Text hourCountGroup : hourCountGroups) {
            final String[] hourGroupBatch = hourCountGroup.toString().split(",");

            for (final String hourCount : hourGroupBatch) {
                final String[] splitted = hourCount.split("\\|");
                final String hour = splitted[0];
                final int count = Integer.parseInt(splitted[1]);

                total += count;

                if (!hourCountMap.containsKey(hour)) {
                    hourCountMap.put(hour, 0);
                }

                hourCountMap.put(hour, hourCountMap.get(hour) + count);
            }
        }

        Set<String> hourCountPairs = new HashSet<String>();

        for (Map.Entry<String,Integer> entry : hourCountMap.entrySet()) {
            hourCountPairs.add(entry.getKey() + "|" + entry.getValue().toString());
        }

        final Text combined = new Text(Joiner.on(",").join(hourCountPairs));
        context.write(key, combined);
    }
}
