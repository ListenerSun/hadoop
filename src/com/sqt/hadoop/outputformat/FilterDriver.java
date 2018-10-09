package com.sqt.hadoop.outputformat;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.lib.FilterOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.junit.Test;

/**驱动类
 * @Description:
 * @author: sqt
 * @Date: Created in 2018-09-28  22:30
 */
public class FilterDriver {

    public static void main(String[] args)
        throws IOException, ClassNotFoundException, InterruptedException {
        args = new String[] { FilterDriver.class.getResource("").getPath()+"/logdata", "E:\\hadooptest\\log\\output" };
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);
        job.setJarByClass(FilterDriver.class);

        job.setMapperClass(FilterMapper.class);
        job.setReducerClass(FilterReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(NullWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

        // 要将自定义的输出格式组件设置到 job 中
        job.setOutputFormatClass(OutPutFormat.class);
        FileInputFormat.setInputPaths(job, new Path(args[0]));

        // 虽然我们自定义了 outputformat，但是因为我们的 outputformat 继承自fileoutputformat
        // 而 fileoutputformat 要输出一个_SUCCESS 文件，所以，在这还得指定一个输出目录
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        boolean result = job.waitForCompletion(true);
        System.exit(result ? 0 : 1);

    }
 /*   @Test
    public void test(){
        String path = this.getClass().getResource("").getPath();
        System.out.println(path);
    }*/
}
