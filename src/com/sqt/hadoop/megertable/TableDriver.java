package com.sqt.hadoop.megertable;


import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * @Description:
 * @author: sqt
 * @Date: Created in 2018-10-09  11:15
 */
public class TableDriver {

    public static void main(String[] args)
        throws IOException, ClassNotFoundException, InterruptedException {
        //获取配置信息
        Configuration configuration = new Configuration();
        Job job = Job.getInstance(configuration);

        //指定本jar包所在的路径
        job.setJarByClass(TableDriver.class);

        //指定mapper/reduce类
        job.setMapperClass(TableMapper.class);
        job.setReducerClass(TableReduce.class);

        //指定mapper输出的数据类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(TableBean.class);

        //指定最终输出类型
        job.setOutputKeyClass(TableBean.class);
        job.setOutputValueClass(NullWritable.class);

        //指定job的输入原始文件的路径
        FileInputFormat.setInputPaths(job,new Path(args[0]));
        FileOutputFormat.setOutputPath(job,new Path(args[1]));
        //  将 job 中配置的相关参数，以及 job 所用的 java 类所在的 jar 包， 提交给 yarn 去运行

        boolean result = job.waitForCompletion(true);
        System.exit(result ? 0 : 1);

    }
}
