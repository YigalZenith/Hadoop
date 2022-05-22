package com.imooc.bigdata.hadoop.mr.access;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class AccessMapper extends Mapper<LongWritable, Text, Text, Access> {
    @Override
    protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, Access>.Context context) throws IOException, InterruptedException {
        String[] fields = value.toString().split("\t");

        // 获取手机号
        String phone = fields[1];
        // 获取本行的上行流量
        Long up = Long.parseLong(fields[fields.length - 3]);
        // 获取本行的下行流量
        Long down = Long.parseLong(fields[fields.length - 2]);

        // 创建一个对象报错本行的手机号、上行流量、下行流量
        Access access = new Access(phone, up, down);

        // 把手机号和access对象交给Reducer
        context.write(new Text(phone), access);

    }
}
