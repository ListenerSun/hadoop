package com.sqt.hadoop.inputformat;

import java.io.IOException;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * @Description:
 * @author: sqt
 * @Date: Created in 2018-09-28  15:07
 */
public class SequenceFileReducer extends Reducer<Text, BytesWritable, Text,
    BytesWritable> {
    @Override
    protected void reduce(Text key, Iterable<BytesWritable> values, Context context)
        throws IOException, InterruptedException {
        context.write(key, values.iterator().next());
    }
}

