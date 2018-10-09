package com.sqt.hadoop.outputformat;

import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * @Description:
 * @author: sqt
 * @Date: Created in 2018-09-28  22:25
 */
public class FilterMapper  extends Mapper<LongWritable, Text, Text, NullWritable> {

    Text k = new Text();//自定义一个Text接收读取的文本内容
    @Override
    protected void map(LongWritable key, Text value, Context context)
        throws IOException, InterruptedException {
        // 1 获取一行
        String line = value.toString();
       //设置key
        k.set(line);
        // 3 写出
        context.write(k, NullWritable.get());
    }
}
