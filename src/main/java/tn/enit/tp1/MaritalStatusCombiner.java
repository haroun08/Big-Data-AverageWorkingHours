package tn.enit.tp1;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class MaritalStatusCombiner extends Reducer<Text, NumPair, Text, NumPair> {
    public void reduce(Text key, Iterable<NumPair> values, Context context) throws IOException, InterruptedException {
        int totalSum = 0;
        int totalCount = 0;

        for (NumPair value : values) {
            totalSum += value.getSum();
            totalCount += value.getCount();
        }

        context.write(key, new NumPair(totalSum, totalCount));
    }
}
