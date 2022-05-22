package com.imooc.bigdata.hadoop.project.mr;

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

public class PVStatApp {
    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
//        System.setProperty("hadoop.home.dir", "C:\\Users\\xxx\\Desktop\\winutils-master\\hadoop-2.6.0");
        Configuration configuration = new Configuration();

        Job job = Job.getInstance(configuration);
        job.setJarByClass(PVStatApp.class);

        job.setMapperClass(MyMapper.class);
        job.setReducerClass(MyReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LongWritable.class);

        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(LongWritable.class);

        FileSystem fileSystem = FileSystem.get(configuration);
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
        Text k = new Text("key");
        LongWritable v = new LongWritable(1);

        @Override
        protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, LongWritable>.Context context) throws IOException, InterruptedException {
            context.write(k, v);
        }
    }

    static class MyReducer extends Reducer<Text, LongWritable, NullWritable, LongWritable> {
        @Override
        protected void reduce(Text key, Iterable<LongWritable> values, Reducer<Text, LongWritable, NullWritable, LongWritable>.Context context) throws IOException, InterruptedException {
            int count = 0;
            for (LongWritable v : values) {
                count++;
            }
            context.write(NullWritable.get(), new LongWritable(count));
        }
    }
}
