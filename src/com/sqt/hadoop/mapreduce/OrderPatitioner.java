package com.sqt.hadoop.mapreduce;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Partitioner;

/**分区
 * @Description:
 * @author: sqt
 * @Date: Created in 2018-09-27  16:19
 */
public class OrderPatitioner extends Partitioner<OrderBean,NullWritable> {

    @Override
    public int getPartition(OrderBean orderBean, NullWritable nullWritable, int numPartitions) {
        return (orderBean.getOrder_id() & Integer.MAX_VALUE) % numPartitions;
    }
}
