package com.sqt.hadoop.inputformat;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

/**
 * @Description:
 * @author: sqt
 * @Date: Created in 2018-09-28  15:08
 */
public class SequenceFileDriver {

    public static void main(String[] args)
        throws IOException, ClassNotFoundException, InterruptedException {

        args = new String[] { "E:\\hadooptest\\SequenceFileData", "E:\\hadooptest\\SequenceFileData\\output" };

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);
        job.setJarByClass(SequenceFileDriver.class);

        // 设置输入的 inputFormat
        job.setInputFormatClass(WholeFileInputformat.class);
        // 设置输出的 outputFormat
        job.setOutputFormatClass(SequenceFileOutputFormat.class);

        job.setMapperClass(SequenceFileMapper.class);
        job.setReducerClass(SequenceFileReducer.class);


        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(BytesWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(BytesWritable.class);

        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        boolean result = job.waitForCompletion(true);
        System.exit(result ? 0 : 1);

    }

}
