package com.imooc.bigdata.hadoop.mr.wc.hdfs;

import java.io.IOException;
import java.util.Properties;

/**
 * 1.加载配置文件
 * 2.返回 properties 对象
 */
public class PropsUtil {
    private static final Properties properties = new Properties();

    static {
        try {
            properties.load(PropsUtil.class.getClassLoader().getResourceAsStream("wc.properties"));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static Properties getProperties() {
        return properties;
    }

    public static void main(String[] args) {
        System.out.println(properties.getProperty("HDFS_URI"));
    }
}
