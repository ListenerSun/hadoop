package com.sqt.hadoop.mapreduce;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
/*
import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

*/

import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;

/**
 * @Description:
 * @author: sqt
 * @Date: Created in 2018-09-27  16:06
 */
public class OrderMapper  extends Mapper<LongWritable,Text,OrderBean,NullWritable>{

    OrderBean orderBean = new OrderBean();
    @Override
    protected void map(LongWritable key, Text value, Context context)
        throws IOException, InterruptedException {
        String line = value.toString();
        String[] fields = line.split("\t");
        orderBean.setOrder_id(Integer.parseInt(fields[0]));
        orderBean.setPrice(Double.parseDouble(fields[2]));
        context.write(orderBean,NullWritable.get());

    }
}


/*public class OrderMapper extends Mapper<LongWritable, Text, OrderBean, NullWritable> {

    OrderBean bean = new OrderBean();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // 1 读取数据
        String line = value.toString();

        // 2 切割数据
        String[] fields = line.split("\t");

        // Order_0000002	Pdt_03	522.8
        // 3 封装bean对象
        bean.setOrder_id(Integer.parseInt(fields[0]));
        bean.setPrice(Double.parseDouble(fields[2]));

        // 4 写出
        context.write(bean, NullWritable.get());

    }
}
*/