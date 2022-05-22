package com.imooc.bigdata.hadoop.project.mr2;

import com.imooc.bigdata.hadoop.project.uitls.LogParser;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.Map;

public class ETLApp {
    public static void main(String[] args) throws Exception {
        System.setProperty("hadoop.home.dir", "C:\\Users\\xxx\\Desktop\\winutils-master\\hadoop-2.6.0");
        Configuration configuration = new Configuration();

        Job job = Job.getInstance(configuration);
        job.setJarByClass(ETLApp.class);

        job.setMapperClass(MyMapper.class);
        job.setMapOutputKeyClass(NullWritable.class);
        job.setMapOutputValueClass(Text.class);

        FileSystem fileSystem = FileSystem.get(configuration);
//        Path path = new Path("input/ETL");
        Path path = new Path(args[1]);
        if (fileSystem.exists(path)) {
            fileSystem.delete(path, true);
        }
//        FileInputFormat.setInputPaths(job, new Path("input/trackinfo_20130721.data"));
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, path);

        boolean b = job.waitForCompletion(true);
        System.exit(b ? 0 : -1);
    }

    static class MyMapper extends Mapper<LongWritable, Text, NullWritable, Text> {
        LogParser logParser;

        @Override
        protected void setup(Mapper<LongWritable, Text, NullWritable, Text>.Context context) throws IOException, InterruptedException {
            logParser = new LogParser();
        }

        @Override
        protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, NullWritable, Text>.Context context) throws IOException, InterruptedException {
            Map<String, String> dataMap = logParser.parseLog(value.toString());
            if (dataMap != null) {
                String time = dataMap.get("time");
                String ip = dataMap.get("ip");
                String country = dataMap.get("country");
                String province = dataMap.get("province");
                String city = dataMap.get("city");
                String url = dataMap.get("url");
                String topicId = dataMap.get("topicId");

                StringBuilder stringBuilder = new StringBuilder();
                stringBuilder.append(time).append("\t");
                stringBuilder.append(ip).append("\t");
                stringBuilder.append(country).append("\t");
                stringBuilder.append(province).append("\t");
                stringBuilder.append(city).append("\t");
                stringBuilder.append(topicId).append("\t");
                stringBuilder.append(url);


                context.write(NullWritable.get(),new Text(stringBuilder.toString()));
            }
        }
    }

}
