package com.sqt.hadoop.mapreduce;

import java.io.IOException;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * @Description:
 * @author: sqt
 * @Date: Created in 2018-09-27  16:16
 */
public class OrderReduce  extends Reducer<OrderBean,NullWritable,OrderBean,NullWritable>{

    @Override
    protected void reduce(OrderBean key, Iterable<NullWritable> values, Context context)
        throws IOException, InterruptedException {
        context.write(key,NullWritable.get());
    }
}
