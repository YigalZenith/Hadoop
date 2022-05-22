package com.imooc.bigdata.hadoop.mr.access;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * 需求: 将统计结果按手机号前缀进行区分,并输出到不同的文件中
 * 13* ===> 分区0
 * 15* ===> 分区1
 * other ===> 分区2
 */
public class AccessPartitioner extends Partitioner<Text, Access> {

    @Override
    public int getPartition(Text text, Access access, int numPartitions) {
        if (text.toString().startsWith("13")) {
            return 0;
        }
        if (text.toString().startsWith("15")) {
            return 1;
        } else {
            return 2;
        }
    }
}
