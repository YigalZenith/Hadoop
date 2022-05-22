package com.imooc.bigdata.hadoop.mr.wc;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * KEYIN: Map任务读取数据的Key类型, offset, 是每行数据的起始位置的偏移量, Long类型
 * VALUEIN: Map任务读取数据的Value类型, line, 其实就是每行的字符串, String类型
 * KEYOUT: Map自定义实现输出的Key的类型, 切割后的键值对中的键, String
 * VALUEOUT: Map自定义实现输出的Vaule的类型, 切割后的键值对中的值, Integer
 * <p>
 * Hadoop自定义类型: 支持序列化和反序列化,比JAVA基本数据类型性能更好
 * LongWritable, Text,Text, IntWritable
 */
public class WordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    @Override
    protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, IntWritable>.Context context) throws IOException, InterruptedException {
        // 把value对应的行数据按照指定的分隔符拆开
        String[] words = value.toString().split("\t");

        for (String word : words) {
            // (hello,1) (world,1)
            context.write(new Text(word.toLowerCase()),new IntWritable(1));
        }
    }
}
