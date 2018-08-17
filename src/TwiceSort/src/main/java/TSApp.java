import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class TSApp {


    public static void main(String[] args) throws Exception {
        Configuration conf=new Configuration();
        Job job=Job.getInstance(conf);

        job.setJobName("TWIRCESORT");
        job.setJarByClass(TSApp.class);
        job.setMapperClass(TSMaper.class);
        job.setReducerClass(TSReducer.class);

        job.setPartitionerClass(TSPartitioner.class);
        job.setSortComparatorClass(ComboKeyCompare.class);
        job.setGroupingComparatorClass(CompareGrouping.class);

        FileInputFormat.addInputPath(job,new Path(args[0]));
        FileOutputFormat.setOutputPath(job,new Path(args[1]));

        job.setInputFormatClass(SequenceFileInputFormat.class);

        job.setOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(NullWritable.class);
        job.setMapOutputKeyClass(ComboKey.class);
        job.setNumReduceTasks(3);

        job.waitForCompletion(true);
    }

}

