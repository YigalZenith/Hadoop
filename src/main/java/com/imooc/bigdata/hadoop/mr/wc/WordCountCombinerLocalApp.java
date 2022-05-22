package com.imooc.bigdata.hadoop.mr.wc;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.net.URISyntaxException;

/**
 * 添加combiner操作
 * 统计本地文件的词频: D:\IdeaProjects\hadoop-train-v2\input\wc.txt
 * 结果输出到本地文件: D:\IdeaProjects\hadoop-train-v2\output
 */
public class WordCountCombinerLocalApp {
    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException, URISyntaxException {
        System.setProperty("hadoop.home.dir","C:\\Users\\xxx\\Desktop\\winutils-master\\hadoop-2.6.0");

        // 创建配置
        Configuration configuration = new Configuration();

        // 创建一个Job任务,并关联配置
        Job job = Job.getInstance(configuration);

        // 设置Job使用的主类
        job.setJarByClass(WordCountCombinerLocalApp.class);

        // 设置Job使用的自定义Mapper和Reducer处理类
        job.setMapperClass(WordCountMapper.class);
        job.setReducerClass(WordCountReducer.class);

        // 添加combiner操作,和Reducer操作一样
        job.setCombinerClass(WordCountReducer.class);

        // 设置Mapper输出的key和value的类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        // 设置Reducer输出的key和value的类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        // 设置作业输入和输出路径
        FileInputFormat.setInputPaths(job, new Path("input"));
        FileOutputFormat.setOutputPath(job, new Path("output"));

        // 提交作业
        boolean b = job.waitForCompletion(true);
        System.exit(b ? 0 : -1);
    }
}
