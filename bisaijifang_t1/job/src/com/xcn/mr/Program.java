package com.xcn.mr;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import org.apache.hadoop.io.NullWritable;

import org.apache.hadoop.mapreduce.Job;

import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;



public class Program {


        public static void main(String[] args) throws Exception {
            Configuration conf = new Configuration();
            Job job = Job.getInstance(conf,"Job1");

            job.setJarByClass(Program.class);
            job.setMapperClass(MyMapper.class);
            job.setNumReduceTasks(0);

            job.setMapOutputKeyClass(Bean.class);
            job.setMapOutputValueClass(NullWritable.class);

            FileInputFormat.addInputPath(job,new Path(args[0]));
            FileOutputFormat.setOutputPath(job,new Path(args[1]));

            System.exit(job.waitForCompletion(true)?0:1);

        }
    }

