package com.imooc.bigdata.hadoop.mr.wc.hdfs;

/**
 * 实现自定义的Mapper接口,对分词的内容忽略大小写
 */
public class IgnoreCaseWcMapper implements ImoocMapper{
    @Override
    public void mapper(String line, ImoocContext context) {
        String[] s = line.toLowerCase().split(" ");
        for (String word: s) {
            Object o = context.Get(word);
            if (o == null){
                context.Set(word,1);
            }else {
                int i = Integer.parseInt(o.toString());
                context.Set(word, i + 1);
            }  
        }
    }
}
