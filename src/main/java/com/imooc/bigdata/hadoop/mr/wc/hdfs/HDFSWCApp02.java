package com.imooc.bigdata.hadoop.mr.wc.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.Map;
import java.util.Properties;

/**
 * 改造HDFSWCApp01
 * 1.把相关字符串自定义成配置文件
 * 2.利用反射创建自定义Mapper对象
 */
public class HDFSWCApp02 {
    public static void main(String[] args) throws Exception {
        // 创建Properties对象
        Properties properties = PropsUtil.getProperties();

        // 1.读取HDFS上的文件
        FileSystem fs = FileSystem.get(new URI(properties.getProperty(Constants.HDFS_URI)),new Configuration(),"hadoop");
        RemoteIterator<LocatedFileStatus> iterator = fs.listFiles(new Path(properties.getProperty(Constants.INPUT_PATH)), false);

        // 通过反射创建自定义Mapper对象
        Class<?> aClass = Class.forName(properties.getProperty(Constants.MAPPER_CLASS));
        ImoocMapper imoocMapper = (ImoocMapper) aClass.newInstance();

        // 初始化缓存Context
        ImoocContext context = new ImoocContext();

        while (iterator.hasNext()) {
            LocatedFileStatus locatedFileStatus = iterator.next();
            FSDataInputStream in = fs.open(locatedFileStatus.getPath());
            BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(in));
            String line;
            while ((line = bufferedReader.readLine()) != null) {
                // 2.业务处理(词频统计):对文件中的每一行数据都要进行业务处理(按照分隔符分割)
                // 3.将处理结果缓存起来
                imoocMapper.mapper(line, context);
            }
            bufferedReader.close();
            in.close();
        }

        // 获取wcMap
        Map<Object, Object> wcMap = context.GetMap();

        // 4.将结果输出到HDFS
        FSDataOutputStream out = fs.create(new Path(properties.getProperty(Constants.OUTPUT_PATH)));
        for (Map.Entry<Object, Object> entry : wcMap.entrySet()) {
            System.out.println(entry.getKey().toString() + ":" + entry.getValue().toString());
            out.write((entry.getKey().toString() + ":" + entry.getValue().toString() + "\n").getBytes());
        }
        out.flush();
        out.close();
    }
}
