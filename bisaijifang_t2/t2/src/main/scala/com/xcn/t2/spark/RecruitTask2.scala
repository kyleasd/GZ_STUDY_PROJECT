package com.xcn.t2.spark

import org.apache.spark.{SparkConf, SparkContext}

object RecruitTask2 {

  // 实验任务二：将薪资格式统一为“下限-上限”，对个别下限大于上限的数据进行次序调整
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("RecruitSpark2").setMaster("spark://master:7077")
    val sc = new SparkContext(conf)

    val accum = sc.longAccumulator("RecruitSpark2")

    // 1 对输入的每一行数据按|进行切割
      val rdd = sc.textFile("hdfs://master:9000/xcn/output/recruitspark1/part-00000")
        .map(x => x.split("\\|"))
    // 2 对薪资的两个字段进行大小比较，使用累加器记录个别下限大于上限的数据条数，
    // 将两个薪资字段合并为下限-上限的格式
        .map( x => {
          var cost = ""
          if( x(5).toInt >= x(6).toInt ){
            cost = x(5) + "-" + x(6)
          }else{
            accum.add(1)
            cost = x(6) + "-" + x(5)
          }
          Array(x(0),x(1),x(2),x(3),x(4),cost,x(7),x(8),x(9),x(10),x(11))

        })
    // 3 遍历上一步返回的结果，将结果数组中的数据用|拼凑成字符串
        .map( x => {
          var result = ""
          for( y <- x ){
            if( y.equals(x(10)) ){
              result += y
            }else{
              result += y + "|"
            }
          }
            result
        }).repartition(1).saveAsTextFile("hdfs://master:9000/xcn/output/recruitspark2")
     println("----------修改记录数为："+accum.value+"--------------------")
  }
}
