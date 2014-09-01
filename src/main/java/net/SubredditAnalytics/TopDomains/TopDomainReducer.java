package net.SubredditAnalytics.TopDomains;

import com.google.common.base.Joiner;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeSet;

/**
 * Created by tom on 8/31/14.
 */
public class TopDomainReducer extends Reducer<Text,Text,Text,Text> {
    public void reduce(Text key, Iterable<Text> values, Context context) throws InterruptedException, IOException{
        Map<String,Integer> wordCount = new HashMap<String,Integer>();

        for (Text valueList : values) {
            for (String value : valueList.toString().split(",")) {
                final String domain = value.split("\\|")[0];
                final Integer count = Integer.parseInt(value.split("\\|")[1]);

                if (!wordCount.containsKey(domain)) {
                    wordCount.put(domain, 0);
                }

                wordCount.put(domain, wordCount.get(domain) + count);
            }
        }

        TreeSet<String> sortedDomainSet = new TreeSet<String>(new DomainCountComparator());

        for (final Map.Entry<String, Integer> entry : wordCount.entrySet()) {
            sortedDomainSet.add(entry.getKey() + "|" + entry.getValue().toString());

            if (sortedDomainSet.size() > 10) {
                sortedDomainSet.pollFirst();
            }
        }
        final String topDomains = Joiner.on(",").join(sortedDomainSet.descendingIterator());
        context.write(key, new Text(topDomains));
    }
}

class DomainCountComparator implements Comparator<String> {
    @Override
    public int compare(final String domainCount1, final String domainCount2) {
        final int c1 = Integer.parseInt(domainCount1.split("\\|")[1]);
        final int c2 = Integer.parseInt(domainCount2.split("\\|")[1]);

        return c1 > c2 ? 1 : -1;
    }
}