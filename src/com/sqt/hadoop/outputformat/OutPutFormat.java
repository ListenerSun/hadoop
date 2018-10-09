package com.sqt.hadoop.outputformat;

import java.io.IOException;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**自定义一个OutPutFormat 来获得RecordWriter()
 * @Description:
 * @author: sqt
 * @Date: Created in 2018-09-28  22:07
 */
public class OutPutFormat  extends FileOutputFormat<Text, NullWritable> {

    @Override
    public RecordWriter<Text, NullWritable> getRecordWriter(TaskAttemptContext job)
        throws IOException, InterruptedException {
        return new FilterRecordWriter(job);
    }
}
