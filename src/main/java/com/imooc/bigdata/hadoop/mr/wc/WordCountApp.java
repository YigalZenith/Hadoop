package com.imooc.bigdata.hadoop.mr.wc;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

/**
 * Driver: 配置Mapper、Reducer的相关属性
 * 使用MR统计HDFS上的文件对应的词频
 * 统计结果输出到HDFS
 * 提交到本地运行: 开发过程使用
 */
public class WordCountApp {
    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException, URISyntaxException {
        // 打印所有配置
        //    Properties properties = System.getProperties();
        //    Enumeration<Object> keys = properties.keys();
        //    while (keys.hasMoreElements()){
        //        String key = keys.nextElement().toString();
        //        String value = properties.getProperty(key);
        //        System.out.println(key+"============"+value);
        //    }

        // 加载源码
        // 下载: https://archive.apache.org/dist/hadoop/common/hadoop-2.6.0/hadoop-2.6.0-src.tar.gz
        // 解压tar.gz然后压缩成.zip
        // 放到任意目录: 例如 D:\Program Files\apache-maven-3.8.5\repository\org\apache\hadoop\hadoop-2.6.0-src.zip
        // 方式1: 项目上右键 -> Open Module Settings -> SDKS -> Source Path
        // 方式2: 打开任意源码文件(例如:org.apache.hadoop.mapreduce.Mapper), 窗口右上角选择choose source

        // windows运行hadoop客户端需要 winutils.exe 和 hadoop.dll
        //     C:\Users\Viper\Desktop\winutils-master\hadoop-2.6.0\bin\winutils.exe
        //     C:\Windows\System32\hadoop.dll
        //     下载地址: https://github.com/steveloughran/winutils
        System.setProperty("hadoop.home.dir","C:\\Users\\xxx\\Desktop\\winutils-master\\hadoop-2.6.0");
        System.setProperty("HADOOP_USER_NAME", "hadoop");

        // 创建配置
        Configuration configuration = new Configuration();
        configuration.set("fs.defaultFS", "hdfs://hadoop000:8020");

        // 创建一个Job任务,并关联配置
        Job job = Job.getInstance(configuration);

        // 设置Job使用的主类
        job.setJarByClass(WordCountApp.class);

        // 设置Job使用的自定义Mapper和Reducer处理类
        job.setMapperClass(WordCountMapper.class);
        job.setReducerClass(WordCountReducer.class);

        // 设置Mapper输出的key和value的类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        // 设置Reducer输出的key和value的类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        // 设置作业输入和输出路径
        FileInputFormat.setInputPaths(job, new Path("/wordcount/input"));
        FileOutputFormat.setOutputPath(job, new Path("/wordcount/output"));

        // 判断输出路径是否已存在,存在则删除
        FileSystem fileSystem = FileSystem.get(new URI("hdfs://hadoop000:8020"), configuration, "hadoop");
        Path path = new Path("/wordcount/output");
        if (fileSystem.exists(path)) {
            fileSystem.delete(path,true);
        }

        // 提交作业
        boolean b = job.waitForCompletion(true);
        System.exit(b ? 0 : -1);
    }
}
