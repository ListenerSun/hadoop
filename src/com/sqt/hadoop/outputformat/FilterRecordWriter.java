package com.sqt.hadoop.outputformat;

import java.io.IOException;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

/**自定义一个FilterRecordWriter继承RecordWriter
 * @Description:
 * @author: sqt
 * @Date: Created in 2018-09-28  22:10
 */
public class FilterRecordWriter  extends RecordWriter<Text, NullWritable> {

    FSDataOutputStream sqtOut = null;
    FSDataOutputStream otherOut = null;
    public FilterRecordWriter(TaskAttemptContext job) {
        // 1 获取文件系统
        FileSystem fs;
        try {
            fs = FileSystem.get(job.getConfiguration());
            // 2 创建输出文件路径
            Path atguiguPath = new Path("E:\\hadooptest\\log\\sqt.log");
            Path otherPath = new Path("E:\\hadooptest\\log\\other.log");
            // 3 创建输出流
            sqtOut = fs.create(atguiguPath);
            otherOut = fs.create(otherPath);
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

   //根据需求写出文件
    @Override
    public void write(Text key, NullWritable value) throws IOException, InterruptedException {
        // 判断是否包含“atguigu”输出到不同文件
        if (key.toString().contains("sqt")) {
            sqtOut.write(key.toString().getBytes());
        } else {
            otherOut.write(key.toString().getBytes());
        }

    }
    //关闭资源
    @Override
    public void close(TaskAttemptContext context) throws IOException, InterruptedException {
        if (sqtOut != null) {
            sqtOut.close();
        }
        if (otherOut != null) {
            otherOut.close();
        }

    }
}
