package com.xcn.spark

import org.apache.spark.{SparkConf, SparkContext}

object myProgram {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("myProgram")
    val sc = new SparkContext(conf)

    val accum = sc.longAccumulator("accum");

    val rdd = sc.textFile("/input/mysql.csv")
    //var sum = 0

    val deal = rdd.filter( line => {
      var count = 0
      val fields = line.split(",")
      fields.foreach( word => {
        if( word == "NULL" )
          count = count + 1
      })
      if( count > 3 ){
        accum.add(1)
        false
      }
      else
        true
    })


    deal.repartition(1).saveAsTextFile(args(0))
    println("----------------------" + accum.value + "----------------------")
  }
}
