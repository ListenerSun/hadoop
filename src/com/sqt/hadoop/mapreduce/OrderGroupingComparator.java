package com.sqt.hadoop.mapreduce;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * 分组
 * @Description:
 * @author: sqt
 * @Date: Created in 2018-09-27  16:30
 */
public class OrderGroupingComparator  extends WritableComparator {

    protected OrderGroupingComparator() {
        super(OrderBean.class,true);
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        OrderBean aBean = (OrderBean) a;
        OrderBean bBean = (OrderBean) b;
        Integer a_order_id = aBean.getOrder_id();
        Integer b_order_id = bBean.getOrder_id();
        return  a_order_id.compareTo(b_order_id);
    }

    /*public static void main(String[] args) {
        String s1 = "12";
        String s2 = "2";
        System.out.println(s1.compareTo(s2));
    }*/
}
