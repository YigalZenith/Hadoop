package com.imooc.bigdata.hadoop.project.mr2;

import com.imooc.bigdata.hadoop.project.uitls.LogParser;
import com.imooc.bigdata.hadoop.project.uitls.LogParserV2;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.Map;

public class ProvinceStatApp {
    public static void main(String[] args) throws Exception {
        System.setProperty("hadoop.home.dir", "C:\\Users\\xxx\\Desktop\\winutils-master\\hadoop-2.6.0");
        Configuration configuration = new Configuration();

        Job job = Job.getInstance(configuration);
        job.setJarByClass(ProvinceStatApp.class);

        job.setMapperClass(MyMapper.class);
        job.setReducerClass(MyReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LongWritable.class);

        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(LongWritable.class);

        FileSystem fileSystem = FileSystem.get(configuration);
//        Path path = new Path("output/v2/provinceStat");
        Path path = new Path(args[1]);
        if (fileSystem.exists(path)) {
            fileSystem.delete(path, true);
        }
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, path);

        boolean b = job.waitForCompletion(true);
        System.exit(b ? 0 : -1);
    }

    static class MyMapper extends Mapper<LongWritable, Text, Text, LongWritable> {
        private LogParserV2 logParserV2;
        private LongWritable v;

        @Override
        protected void setup(Mapper<LongWritable, Text, Text, LongWritable>.Context context) throws IOException, InterruptedException {
            logParserV2 = new LogParserV2();
            v = new LongWritable(1);
        }

        @Override
        protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, LongWritable>.Context context) throws IOException, InterruptedException {
            Map<String, String> dataMap = logParserV2.parseLog(value.toString());
            if (dataMap != null) {
                String province = dataMap.get("province");
                if (StringUtils.isNotBlank(province)){
                    context.write(new Text(province),v);
                } else {
                    context.write(new Text("-"),v);
                }
            } else {
                context.write(new Text("-"),v);
            }
        }
    }

    static class MyReducer extends Reducer<Text, LongWritable, Text,LongWritable>{
        @Override
        protected void reduce(Text key, Iterable<LongWritable> values, Reducer<Text, LongWritable, Text, LongWritable>.Context context) throws IOException, InterruptedException {
            int count = 0;
            for (LongWritable num:values) {
                count++;
            }
            context.write(key,new LongWritable(count));
        }
    }
}
