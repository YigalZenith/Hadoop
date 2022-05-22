package com.imooc.bigdata.hadoop.mr.access;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * 自定义复杂数据类型
 * 1) 按照Hadoop的规范, 需要实现 Writable 接口
 * 2) 按照Hadoop的规范, 需要实现 write 和 readFields 方法
 * 3) 定义一个默认的构造方法
 */
public class Access implements Writable {
    private String phone;
    private Long up;
    private Long down;
    private Long sum;

    // 默认构造方法
    public Access() {
    }

    // 自定义构造方法，用来初始化对象
    public Access(String phone, Long up, Long down) {
        this.phone = phone;
        this.up = up;
        this.down = down;
        this.sum = up + down;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(phone);
        out.writeLong(up);
        out.writeLong(down);
        out.writeLong(sum);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        phone = in.readUTF();
        up = in.readLong();
        down = in.readLong();
        sum = in.readLong();
    }

    // 对象的输出格式
    @Override
    public String toString() {
        return phone + " " + up + " " + down + " " + sum;
    }

    public String getPhone() {
        return phone;
    }

    public void setPhone(String phone) {
        this.phone = phone;
    }

    public Long getUp() {
        return up;
    }

    public void setUp(Long up) {
        this.up = up;
    }

    public Long getDown() {
        return down;
    }

    public void setDown(Long down) {
        this.down = down;
    }

    public Long getSum() {
        return sum;
    }

    public void setSum(Long sum) {
        this.sum = sum;
    }
}
