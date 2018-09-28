package com.sqt.hadoop.inputformat;

import java.io.IOException;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

/**
 * @Description:
 * @author: sqt
 * @Date: Created in 2018-09-28  15:04
 */
public class SequenceFileMapper extends Mapper<NullWritable, BytesWritable, Text,BytesWritable> {


    Text k = new Text();
    @Override
    protected void setup(Mapper<NullWritable, BytesWritable, Text, BytesWritable>.Context
        context)
        throws IOException, InterruptedException {
        // 1 ��ȡ�ļ���Ƭ��Ϣ
        FileSplit inputSplit = (FileSplit) context.getInputSplit();
        // 2 ��ȡ��Ƭ����
        String name = inputSplit.getPath().toString();
        // 3 ���� key �����
        k.set(name);
    }
    @Override
    protected void map(NullWritable key, BytesWritable value,
        Context context)
        throws IOException, InterruptedException {
        context.write(k, value);
    }

}
