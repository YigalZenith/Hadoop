package com.imooc.bigdata.hadoop.mr.wc.hdfs;

/**
 * 实现自定义的Mapper接口
 */
public class WcMapper implements ImoocMapper{
    @Override
    public void mapper(String line, ImoocContext context) {
        String[] s = line.split(" ");
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
