package tn.enit.tp1;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class AverageWorkingHours {
    public static void main(String[] args) throws Exception {
        if (args.length < 2) {
            System.err.println("Usage: AverageWorkingHours <input path> <output path>");
            System.exit(-1);
        }

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Average Working Hours by Marital Status");

        job.setJarByClass(AverageWorkingHours.class);
        job.setMapperClass(MaritalStatusMapper.class);
        job.setCombinerClass(MaritalStatusCombiner.class);
        job.setReducerClass(MaritalStatusReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(NumPair.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
