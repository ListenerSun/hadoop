package com.sqt.hadoop.megertable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.Writable;

/**
 * @Description: 创建需要合并表的bean对象
 * @author: sqt
 * @Date: Created in 2018-10-09  9:44
 */
public class TableBean implements Writable {
    private String order_id ; //表id
    private String p_id;      //产品id
    private String amount;       //产品数量
    private String pname;     // 产品名称
    private String flag;      // 表的标记 0:订单表, 1:商品表

   //序列化
    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(order_id);
        out.writeUTF(p_id);
        out.writeUTF(amount);
        out.writeUTF(pname);
        out.writeUTF(flag);
    }

    //序列化
    @Override
    public void readFields(DataInput in) throws IOException {
        this.order_id = in.readUTF();
        this.p_id = in.readUTF();
        this.amount = in.readUTF();
        this.pname = in.readUTF();
        this.flag = in.readUTF();

    }

    @Override
    public String toString() {
        return order_id + "\t" + pname + "\t" + amount ;
    }

    public TableBean() {
        super();
    }

    public TableBean(String order_id, String p_id, String amount, String pname, String flag) {
        this.order_id = order_id;
        this.p_id = p_id;
        this.amount = amount;
        this.pname = pname;
        this.flag = flag;
    }

    public String getOrder_id() {
        return order_id;
    }

    public void setOrder_id(String order_id) {
        this.order_id = order_id;
    }

    public String getP_id() {
        return p_id;
    }

    public void setP_id(String p_id) {
        this.p_id = p_id;
    }

    public String getAmount() {
        return amount;
    }

    public void setAmount(String amount) {
        this.amount = amount;
    }

    public String getPname() {
        return pname;
    }

    public void setPname(String pname) {
        this.pname = pname;
    }

    public String getFlag() {
        return flag;
    }

    public void setFlag(String flag) {
        this.flag = flag;
    }
}
