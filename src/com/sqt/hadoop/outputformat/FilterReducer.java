package com.sqt.hadoop.outputformat;

import java.io.IOException;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**reduce方法
 * @Description:
 * @author: sqt
 * @Date: Created in 2018-09-28  22:28
 */
public class FilterReducer  extends Reducer<Text, NullWritable, Text, NullWritable> {

    @Override
    protected void reduce(Text key, Iterable<NullWritable> values, Context context)
        throws IOException, InterruptedException {
        //让输出的key进行换行。设置value为空
        String k = key.toString();
        k = k + "\r\n";
        context.write(new Text(k), NullWritable.get());
    }
}
