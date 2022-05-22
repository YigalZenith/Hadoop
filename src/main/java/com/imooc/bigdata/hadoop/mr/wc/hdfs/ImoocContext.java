package com.imooc.bigdata.hadoop.mr.wc.hdfs;

import java.util.HashMap;
import java.util.Map;

/**
 * 自定义Context,创建map用来做缓存
 */
public class ImoocContext {
    private final Map<Object,Object> wcMap = new HashMap<>();

    public Map<Object,Object> GetMap(){
        return wcMap;
    }

    public Object Get(Object key){
        return wcMap.get(key);
    }

    public void Set(Object key,Object value){
        wcMap.put(key, value);
    }
}
