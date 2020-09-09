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

public class Mr {
    public static class Mymap extends Mapper<LongWritable,Text,Text,Bean>{
        Text k = new Text();
        Bean v = new Bean();
        //省份、城市、酒店数量、房间数量。
        //SEQ,酒店,国家,省份,城市,商圈,星级,业务部门,房间数,图片数,评分,评论数,城市平均实住间夜,酒店总订单,酒店总间夜,酒店实住订单,酒店实住间夜,酒店直销订单,酒店直销间夜,酒店直销实住订单,酒店直销实住间夜,酒店直销拒单,酒店直销拒单率,城市直销订单,城市直销拒单率,拒单率是否小于等于直销城市均值
        //aba_2066,马尔康嘉绒大酒店,中国,四川,阿坝,NULL,四星级/高档,OTA,85,NULL,4.143799782,108,34.06,45,75,22,44,NULL,NULL,NULL,NULL,NULL,NULL,34147,7.90%,0


        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String[] fields = line.split(",");

            if( !(fields[3].equals("NULL")) && !(fields[4].equals("NULL")) && !(fields[8].equals("NULL")) && !(fields[8].equals("房间数")) ) {
                k.set(fields[3] + "\t" + fields[4]);
                v.set(1, Long.parseLong(fields[8]));
                context.write(k,v);
            }

        }
    }

    public static class MyReduce extends Reducer<Text,Bean,Text,Bean>{

        Bean v = new Bean();

        @Override
        protected void reduce(Text key, Iterable<Bean> values, Context context) throws IOException, InterruptedException {
            long hotel_num = 0;
            long room_num = 0;
            for( Bean val : values ){
                hotel_num++ ;
                room_num += val.getRoom_num();
            }
            v.setHotel_num(hotel_num);
            v.setRoom_num(room_num);
            context.write(key,v);
        }
    }


        public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
            Configuration conf = new Configuration();
            Job job = Job.getInstance(conf,"h3cuMr1");

            job.setJarByClass(Mr.class);
            job.setMapperClass(Mymap.class);
            job.setReducerClass(MyReduce.class);

            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(Bean.class);

            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Bean.class);

            FileInputFormat.setInputPaths(job,new Path(args[0]));
            FileOutputFormat.setOutputPath(job,new Path(args[1]));

            System.exit(job.waitForCompletion(true)?0:1);
        }

}
