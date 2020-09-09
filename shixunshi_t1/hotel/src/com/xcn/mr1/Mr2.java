package com.xcn.mr1;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class Mr2 {
    public static class Mymap extends Mapper<LongWritable, Text, Bean,Text > {
        Bean k = new Bean();
        Text v = new Text();
        //省份、城市、酒店数量、房间数量。
        //内蒙古	阿拉善盟	278	8780

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String[] fields = line.split(",");
            k.set(Long.parseLong(fields[2]),Long.parseLong(fields[3]));
            v.set(fields[0] + "\t" + fields[1]);

            context.write(k,v);

        }
    }

    public static class MyReduce extends Reducer<Bean, Text, Text, Bean> {

        @Override
        protected void reduce( Bean key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            Text val = values.iterator().next();
            context.write(val,key);
        }
    }


    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "h3cuMr1");

        job.setJarByClass(Mr.class);
        job.setMapperClass(Mr.Mymap.class);
        job.setReducerClass(Mr.MyReduce.class);

        job.setMapOutputKeyClass(Bean.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Bean.class);

        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
