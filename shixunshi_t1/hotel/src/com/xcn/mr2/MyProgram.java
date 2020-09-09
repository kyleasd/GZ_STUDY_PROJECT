package com.xcn.mr2;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class MyProgram {
    //省份、直销拒单率
    //SEQ,酒店,国家,省份,城市,商圈,星级,业务部门,房间数,图片数,评分,评论数,城市平均实住间夜,酒店总订单,酒店总间夜,酒店实住订单,酒店实住间夜,酒店直销订单,酒店直销间夜,酒店直销实住订单,酒店直销实住间夜,酒店直销拒单,酒店直销拒单率,城市直销订单,城市直销拒单率,拒单率是否小于等于直销城市均值
    //anqing_2733,安庆天柱山蓝天宾馆,中国,安徽,安庆,天柱山,二星及其他,低星,13,NULL,3.445650101,6,18.00,1,1,1,1,1,1,1,1,1,100.00%,6207,6.11%,0
    static class MyMap extends Mapper<LongWritable, Text, Text,FloatWritable>{
        Text k = new Text();
        FloatWritable v = new FloatWritable();

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String[]fields = line.split(",");

            if( !(fields[3].equals("NULL")) && !(fields[fields.length - 3].equals("NULL")) && !(fields[fields.length - 3].equals("城市直销订单")) ) {
                float refuse = Float.parseFloat(fields[fields.length - 3].substring(0,3));
                k.set(fields[3]);
                v.set(refuse);
                context.write(k, v);
            }
        }
    }

    static class MyReduce extends Reducer<Text,FloatWritable,Text,Text>{
        Text v = new Text();

        @Override
        protected void reduce(Text key, Iterable<FloatWritable> values, Context context) throws IOException, InterruptedException {
                int count = 0;
                float f = 0;
                for (FloatWritable val : values){
                    count++;
                    f += val.get() / 100;
                }
                float avg = f / count;

            v.set(String.format("%f",avg));
            context.write(key,v);
        }
    }


        public static void main(String[] args) throws Exception{
            Configuration conf = new Configuration();
            Job job = Job.getInstance(conf,"hotelMr2");
            System.out.println("this is : " + job.getJobName());

            job.setJarByClass(MyProgram.class);
            job.setMapperClass(MyMap.class);
            job.setReducerClass(MyReduce.class);

            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(FloatWritable.class);

            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);

            FileInputFormat.setInputPaths(job,new Path("/output/hotelsparktask1/part-00000"));
            FileOutputFormat.setOutputPath(job,new Path(args[0]));

            System.exit(job.waitForCompletion(true)?0:1);
        }

}
