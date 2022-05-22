package com.imooc.bigdata.hadoop.mr.wc.hdfs;

/**
 * 自定义Mapper接口
 */
public interface ImoocMapper {
    /**
     * @param line: 要切割的内容
     * @param context: 自定义Context(用来做缓存)
     */
    void mapper(String line, ImoocContext context);
}
