package com.sqt.hadoop.inputformat;



import java.io.IOException;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

/**定义类继承 FileInputFormat
 * @Description:
 * @author: sqt
 * @Date: Created in 2018-09-28  10:37
 */
public class WholeFileInputformat extends FileInputFormat<NullWritable,BytesWritable>{

   //是否可以切割
    @Override
    protected boolean isSplitable(JobContext context, Path filename) {
        return super.isSplitable(context, filename);
    }

    @Override
    public RecordReader<NullWritable, BytesWritable> createRecordReader(InputSplit split,
        TaskAttemptContext context) throws IOException, InterruptedException {
        WholeRecordReader reader = new WholeRecordReader();
        //调用初始化方法
        reader.initialize(split,context);
        return reader;
    }
}
