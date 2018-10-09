package com.sqt.hadoop.megertable;

import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

/**
 * @Description:
 * @author: sqt
 * @Date: Created in 2018-10-09  9:53
 */
public class TableMapper extends Mapper<LongWritable, Text, Text, TableBean> {
    TableBean tableBean = new TableBean();
    Text k = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context)
        throws IOException, InterruptedException {
        //获取文件的名称
        FileSplit split = (FileSplit) context.getInputSplit();
        String name = split.getPath().getName();

        String line = value.toString();

        //不同名字对应不同的表
        if (name.startsWith("order")){//订单表处理
            String[] fields = line.split("\t");
            //封装bean对象
            tableBean.setOrder_id(fields[0]);
            tableBean.setP_id(fields[1]);
            tableBean.setAmount(fields[2]);
            tableBean.setPname("");
            tableBean.setFlag("0");

            k.set(fields[1]);
        }else {//商品表处理
            String[] fields = line.split("\t");
            //封装对象
            tableBean.setP_id(fields[0]);
            tableBean.setPname(fields[1]);
            tableBean.setFlag("1");
            tableBean.setAmount("0");
            tableBean.setOrder_id("");
            k.set(fields[0]);
        }

        //写出
        context.write(k,tableBean);

    }
}
