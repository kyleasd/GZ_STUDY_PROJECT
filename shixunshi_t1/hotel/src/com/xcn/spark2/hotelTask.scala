package com.xcn.spark2

import org.apache.spark.{SparkConf, SparkContext}

object hotelTask {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("hotelTask")
    val sc = new SparkContext(conf)
    //SEQ,酒店,国家,省份,城市,商圈,星级,业务部门,房间数,图片数,评分,评论数,城市平均实住间夜,酒店总订单,酒店总间夜,酒店实住订单,酒店实住间夜,酒店直销订单,酒店直销间夜,酒店直销实住订单,酒店直销实住间夜,酒店直销拒单,酒店直销拒单率,城市直销订单,城市直销拒单率,拒单率是否小于等于直销城市均值
    // 6 10 11
    val rdd = sc.textFile("/input/mysql.csv")
    var sum = 0

    var deal = rdd.filter(line => {

      val fields = line.split(",")
      if (fields(6).equals("NULL")) {
        sum = sum + 1
        false
      }
      else if (fields(10).equals("NULL")) {
        sum = sum + 1
        false
      }
      else if (fields(11).equals("NULL")) {
        sum = sum + 1
        false
      }
      else
        true
    })

    deal.repartition(1).saveAsTextFile(args(0))


  }
}
