package com.sqt.hadoop.mapreduce;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.WritableComparable;

/**
 * @Description:
 * @author: sqt
 * @Date: Created in 2018-09-27  15:29
 */
public class OrderBean implements WritableComparable<OrderBean> {

    private int order_id;//订单编号
    private double price;

    //添加空构造方法，反射用
    public OrderBean() {
        super();
    }

    public OrderBean(int order_id, double price) {
        this.order_id = order_id;
        this.price = price;
    }

    //在map阶段排序定义

   //序列化
    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeInt(order_id);
        dataOutput.writeDouble(price);
    }
    //反序列化
    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.order_id = dataInput.readInt();
        this.price = dataInput.readDouble();
    }

    @Override
    public String toString() {
        return order_id + "\t" + price ;
    }

    public int getOrder_id() {
        return order_id;
    }

    public void setOrder_id(int order_id) {
        this.order_id = order_id;
    }

    public double getPrice() {
        return price;
    }

    public void setPrice(double price) {
        this.price = price;
    }

    //二次排序
    @Override
    public int compareTo(OrderBean o) {
        int result ;
        if (order_id > o.getOrder_id()) {
            result = 1;
        } else if (order_id < o.getOrder_id()) {
            result = -1;
        } else {
            // 价格倒序排序
            result = price > o.getPrice() ? -1 : 1;
        }
        return result;
    }
}
