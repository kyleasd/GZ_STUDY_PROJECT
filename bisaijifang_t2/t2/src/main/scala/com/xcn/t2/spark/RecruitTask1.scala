package com.xcn.t2.spark

import com.alibaba.fastjson.JSON
import org.apache.spark.{SparkConf, SparkContext}

object RecruitTask1 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local").setAppName("RecruitTask1")
    val sc = new SparkContext(conf)

    // 1.创建累加器，用来保存剔除的记录条数
    val accum = sc.longAccumulator("RecruitSpark1")

    val rdd1 = sc.textFile("hdfs://master:9000/xcn/input/recruit.json")
    // 2.由于字段中存在\r和\n这些特殊符号，需要删除这些特殊符号，否则会影响后续的清洗操作
      .map( x => {
        x.replaceAll("\\\\r|\\\\n","")
      })
    // 3.查看原始数据集，可以看到第一行和最后一行的数据是无意义的，故剔除第一行和最后一行数据
      .filter( x => {
        !(x.contains("[") || x.contains("]"))
      })
    // 4.查看原始数据集，
    // 可以看到每行json数据末尾都有一个逗号，
    // 为了使每行数据都符合json格式，需要剔除每行数据末尾的逗号
      .map( x => {
        var result = ""
        if( x.contains("}},") )
          {
            result = x.replaceAll("}},","}}")
          }else{
          result = x
        }
        result
      })
    // 5.使用fastjson对每行数据进行json解析，并使用数组存放每个字段的数据，返回该数组
      // {"name": "Java工程师",
      // "detail": {
      // "公司名称": "阿里巴巴",
      // "工作城市": "北京",
      // "工作要求": "熟练使用RPC框架，具备相关的分布式开发经验",
      // "招聘人数": 3,
      // "薪资上限": "12000",
      // "薪资下限": "7000",
      // "工作技术": "【Spark, HBase, Linux, Python】",
      // "发布时间": "2019-03-28",
      // "招聘性别": "女",
      // "公司描述": "阿里巴巴网络技术有限公司（简称：阿里巴巴集团）是以曾担任英语教师的马云为首的18人于1999年在浙江杭州创立",
      // "学历要求": " 专科及其以上"}},
      .map( x => {
        val jsonObject = JSON.parseObject(x)
        val name = jsonObject.getOrDefault("name","").toString
        val detail = jsonObject.getOrDefault("detail","").toString
        val dt = JSON.parseObject(detail)
        val company_name = dt.getOrDefault("公司名称","").toString
        val city = dt.getOrDefault("工作城市","").toString
        val require = dt.getOrDefault("工作要求","").toString
        val man = dt.getOrDefault("招聘人数","").toString
        val cost_max = dt.getOrDefault("薪资上限","").toString
        val cost_min = dt.getOrDefault("薪资下限","").toString
        val skill = dt.getOrDefault("工作技术","").toString
        val time = dt.getOrDefault("发布时间","").toString
        val sex = dt.getOrDefault("招聘性别","").toString
        val introduce = dt.getOrDefault("公司描述","").toString
        val need = dt.getOrDefault("学历要求","").toString

        Array(name,company_name,city,require,man,cost_max,cost_min,skill,time,sex,introduce,need)
      })
      // 6.遍历上一步返回的数组，剔除字段为空的数据，即如果数组中含有空的值，则剔除这一行
      .filter( x => {
        var result = true
        for( y <- x ){
          if( y.equals("") || y.equals("null") || y == null){
            result = false
          }
        }
        result
      })
      // 7.剔除不合规的薪资数据，并使用上面定义的累加器存放删除的记录条数
      //  薪资字段如果出现-，则表示薪资存在负值，需要剔除,并使计数器+1
      .filter( x => {
        if( x(5).contains("-") || x(6).contains("-")){
          accum.add(1)
          false
        }else{
          true
        }
      })
      // 8.剔除工作技术字段中的【】
      .map( x => {
        x(7) = x(7).replaceAll("【|】","")
        x
      })
      // 9.遍历上一步返回的结果，将结果数组中的数据用|拼凑成字符串
      .map( x => {
        var result = ""
        for( y <- x ){
          if( y.equals(x(11)) ){
            result += y.trim
          }else{
            result += y.trim + "|"
          }
        }
        result
      })
      // 9.输出最终结果到指定目录
      .repartition(1).saveAsTextFile("hdfs://master:9000/xcn/output/recruitspark1")
      println("----------删除记录数为："+accum.value+"--------------------")

  }

}
