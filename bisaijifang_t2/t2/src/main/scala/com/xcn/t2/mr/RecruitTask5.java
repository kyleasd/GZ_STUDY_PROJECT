package com.xcn.t2.mr;

import org.apache.hadoop.conf.Configuration;
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

public class RecruitTask5 {
    // 实验任务五：统计职位名称中包含“大数据”的所有工作技术标签出现的频次
    // Hadoop, Spark, MySQL, JAVA, Streaming
    public static class MyMap extends Mapper<LongWritable, Text,Text, NullWritable>{
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] fields = value.toString().split(",");

            for ( String val : fields ){
                context.write(new Text(val.trim().toLowerCase()),NullWritable.get());
            }


        }
    }

    public static class MyReduce extends Reducer<Text,NullWritable,Text,NullWritable>{

        @Override
        protected void reduce(Text key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (NullWritable val : values) {
                sum += 1;
            }
            context.write(new Text(key.toString() + "|" + sum),NullWritable.get());
        }
    }


    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);

        job.setJarByClass(RecruitTask5.class);
        job.setMapperClass(MyMap.class);
        job.setReducerClass(MyReduce.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(NullWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

        FileInputFormat.setInputPaths(job,new Path("/xcn/output/wordcount"));
        FileOutputFormat.setOutputPath(job,new Path("/xcn/output/recruitspark5"));

        System.exit(job.waitForCompletion(false)?0:1);


    }



}
