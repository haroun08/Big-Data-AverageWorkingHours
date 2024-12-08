package tn.enit.tp1;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class MaritalStatusMapper extends Mapper<Object, Text, Text, NumPair> {
    private final Text maritalStatus = new Text();

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String[] fields = value.toString().split(",\\s*");

        // Assuming "marital-status" is in the 5th column (adjust index based on the dataset)
        if (fields.length > 6) {
            try {
                maritalStatus.set(fields[5]); // 5th column is marital-status
                int hoursWorked = Integer.parseInt(fields[12].trim()); // 13th column is hours worked
                context.write(maritalStatus, new NumPair(hoursWorked, 1));
            } catch (NumberFormatException e) {
                // Ignore invalid data
            }
        }
    }
}
