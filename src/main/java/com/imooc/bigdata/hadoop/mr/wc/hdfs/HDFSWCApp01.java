package com.imooc.bigdata.hadoop.mr.wc.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;

import java.io.*;
import java.net.URI;
import java.util.Map;

/**
 * 需求: 统计HDFS上的文件的WordCount,然后将统计结果输出到HDFS
 * <p>
 * 功能拆解:
 * 1.读取HDFS上的文件 ===> HDFS API
 * 2.业务处理(词频统计):对文件中的每一行数据都要进行业务处理(按照分隔符分割) ===> Mapper
 * 3.将处理结果缓存起来 ===> Context
 * 4.将结果输出到HDFS ===> HDFS API
 */
public class HDFSWCApp01 {
    public static void main(String[] args) throws Exception {
        // 1.读取HDFS上的文件
        FileSystem fs = FileSystem.get(new URI("hdfs://hadoop000:8020"), new Configuration(), "hadoop");
        RemoteIterator<LocatedFileStatus> iterator = fs.listFiles(new Path("/hdfsApi/wctest"), false);

        // 初始化分词器所在的类WcWrapper
        WcMapper wcMapper = new WcMapper();

        // 初始化缓存Context
        ImoocContext context = new ImoocContext();

        // 迭代目录的所有文件,用自定义Mapper进行处理
        while (iterator.hasNext()) {
            LocatedFileStatus locatedFileStatus = iterator.next();
            FSDataInputStream in = fs.open(locatedFileStatus.getPath());
            BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(in));
            String line;
            while ((line = bufferedReader.readLine()) != null) {
                // 2.业务处理(词频统计):对文件中的每一行数据都要进行业务处理(按照分隔符分割)
                // 3.将处理结果缓存起来
                wcMapper.mapper(line, context);
            }
            bufferedReader.close();
            in.close();
        }

        // 获取wcMap
        Map<Object, Object> wcMap = context.GetMap();

        // 4.将结果输出到HDFS
        FSDataOutputStream out = fs.create(new Path("/wc.txt"));
        for (Map.Entry<Object, Object> entry : wcMap.entrySet()) {
            System.out.println(entry.getKey().toString() + ":" + entry.getValue().toString());
            out.write((entry.getKey().toString() + ":" + entry.getValue().toString() + "\n").getBytes());
        }
        out.flush();
        out.close();
    }
}
