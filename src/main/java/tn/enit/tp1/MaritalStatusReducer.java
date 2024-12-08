package tn.enit.tp1;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class MaritalStatusReducer extends Reducer<Text, NumPair, Text, Text> {
    public void reduce(Text key, Iterable<NumPair> values, Context context) throws IOException, InterruptedException {
        int totalSum = 0;
        int totalCount = 0;

        for (NumPair value : values) {
            totalSum += value.getSum();
            totalCount += value.getCount();
        }

        double average = (double) totalSum / totalCount;
        context.write(key, new Text(String.format("%.2f", average)));
    }
}
