package com.xcn.mr;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class Job {
    public static  class MyMapper extends Mapper<LongWritable, Text,Bean, NullWritable> {
        // {"result": {
        // "公司名称": "阿里巴巴",
        // "工作城市": "广州",
        // "工作要求": "岗位职责：1、负责公司司法事业线系统规划，完成产品线需求规划、功能业务规划、流程设计、产品设计、评估/优化、版本演进，达成产品目标;2、制定详细的产品计划并跟踪好执行情况，把握产品的关键点和里程碑，规避产品风险，按质按量完成项目；3、解决和协调解决产品当中技术问题；4、与客户沟通，解决客户提出各类问题和发掘客户的潜在需求；5、把客户的需求整理成相应的文档，能编写产品方案文档；岗位要求：1、本科以上学历、具备3年以上业务软件系统的需求分析经验，有物联网相关经验者优先；2、具备良好的沟通协调能力，思维逻辑清晰、能独立访谈客户了解需求；3、具备需求分析常用工具使用技能：visio/Axure RP/office/思维导图；4、良好的文字表达与文字理解能力、至少独立负责完成两个以上项目的项目解决方案、需求调研，需求分析、原型设计、需求规格说明书经验；5、有政府、事业单位软件需求分析经验优先；6、可接受省内短期出差。职能类别：需求工程师软件工程师关键字：需求设计分析系统分析五险一金年终奖金双休微信分享",
        // "招聘人数": 14,
        // "工资情况": "5000-10000",
        // "name": "产品经理",
        // "detail": "岗位职责：1、负责公司司法事业线系统规划，完成产品线需求规划、功能业务规划、流程设计、产品设计、评估/优化、版本演进，达成产品目标;2、制定详细的产品计划并跟踪好执行情况，把握产品的关键点和里程碑，规避产品风险，按质按量完成项目；3、解决和协调解决产品当中技术问题；4、与客户沟通，解决客户提出各类问题和发掘客户的潜在需求；5、把客户的需求整理成相应的文档，能编写产品方案文档；岗位要求：1、本科以上学历、具备3年以上业务软件系统的需求分析经验，有物联网相关经验者优先；2、具备良好的沟通协调能力，思维逻辑清晰、能独立访谈客户了解需求；3、具备需求分析常用工具使用技能：visio/Axure RP/office/思维导图；4、良好的文字表达与文字理解能力、至少独立负责完成两个以上项目的项目解决方案、需求调研，需求分析、原型设计、需求规格说明书经验；5、有政府、事业单位软件需求分析经验优先；6、可接受省内短期出差。职能类别：需求工程师软件工程师关键字：需求设计分析系统分析五险一金年终奖金双休微信分享"}},


        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            Bean k = new Bean();
            JSONObject jsonObject1 = JSON.parseObject(value.toString());
            String line = jsonObject1.getOrDefault("result", "").toString();

            if (line != null) {
                JSONObject jsonObject2 = JSON.parseObject(line);
                String company_name = jsonObject2.getOrDefault("公司名称", "").toString();
                String city = jsonObject2.getOrDefault("工作城市", "").toString();
                String require = jsonObject2.getOrDefault("工作要求", "").toString();
                String man = jsonObject2.getOrDefault("招聘人数", "").toString();
                String salary = jsonObject2.getOrDefault("工资情况", "").toString();
                String name = jsonObject2.getOrDefault("name", "").toString();
                String detail = jsonObject2.getOrDefault("detail", "").toString();
                k.setAll(company_name, city, require, man, salary, name, detail);

                if( company_name == null && company_name.equals("") ){
                    return;
                }
                context.write(k, NullWritable.get());

            } else {
                return;
            }


        }
    }



    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();

        org.apache.hadoop.mapreduce.Job job = org.apache.hadoop.mapreduce.Job.getInstance(conf,"Job1");

        job.setJarByClass(Job.class);
        job.setMapperClass(MyMapper.class);
        job.setNumReduceTasks(0);

        job.setMapOutputKeyClass(Bean.class);
        job.setMapOutputValueClass(NullWritable.class);

        FileInputFormat.addInputPath(job,new Path(args[0]));
        FileOutputFormat.setOutputPath(job,new Path(args[1]));

        System.exit(job.waitForCompletion(true)?0:1);

    }
}
