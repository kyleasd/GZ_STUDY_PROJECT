package com.xcn.spark

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql._
import org.apache.spark.sql.types._

object yelp2 {
 def main(args: Array[String]): Unit = {
  val ss = SparkSession.builder().getOrCreate()

  val df = ss.read.parquet("hdfs://master:9000/xcn/output/yelp/init")
  df.createOrReplaceTempView("business")
  val options = Map("header"->"true","inferschema"->"true")

  println("----------------------正在处理categories字段数组========>")
  val part_business = ss.sql("select business_id,state,city,stars,review_count,explode(categories) as category from business")

  //  val all = ss.sql("select business_id,state,city,stars,review_count,explode(categories) as category from business")
  //  all.createOrReplaceTempView("all")

  /* part_business.show(5)
     +--------------------+-----+-------+-----+------------+------------------+
     |         business_id|state|   city|stars|review_count|          category|
     +--------------------+-----+-------+-----+------------+------------------+
     |-2YIg-PAgCMXMLxw5...|   AZ|Phoenix|  4.0|          41|          Notaries|
     |-2YIg-PAgCMXMLxw5...|   AZ|Phoenix|  4.0|          41|   Mailbox Centers|
     |-2YIg-PAgCMXMLxw5...|   AZ|Phoenix|  4.0|          41| Printing Services|
     |-2YIg-PAgCMXMLxw5...|   AZ|Phoenix|  4.0|          41|    Local Services|
     |-2YIg-PAgCMXMLxw5...|   AZ|Phoenix|  4.0|          41|  Shipping Centers|
     +--------------------+-----+-------+-----+------------+------------------+
  */

  val part_business1 = part_business.rdd.map( x => {
   val result = x(5).toString.replace(" ","")
   Row(x(0).toString,x(1).toString,x(2).toString,x(3).toString.toFloat,x(4).toString.toInt,result)
  })

  val pbSchema = StructType(Array(StructField("business_id",StringType,true),StructField("state",StringType,true),StructField("city",StringType,true),StructField("stars",FloatType,true),StructField("review_count",IntegerType,true),StructField("category",StringType,true)))

  val part_business2 = ss.createDataFrame(part_business1,pbSchema)

  part_business2.createOrReplaceTempView("part_business")

  println("----------------------1.商业类别个数========>")
  // 1.商业类别个数
  val all_categories = ss.sql("select count(distinct category) as category from part_business")
  all_categories.write.mode("overwrite").options(options).csv("/xcn/output/yelp/"+"1")

  println("----------------------2.美国10种主要的商业类别========>")
  // 2.美国10种主要的商业类别
  val main_categories = ss.sql("select category,count(*) as num from part_business group by category order by num desc limit 10")
  main_categories.write.mode("overwrite").options(options).csv("/xcn/output/yelp/"+"2")

  println("----------------------3.每个城市各种商业类型的商家数量========>")
  // 3.每个城市各种商业类型的商家数量
  val top_cat_city = ss.sql("select city,category,count(business_id) as num from part_business group by city,category order by num desc")
  top_cat_city.repartition(1).write.mode("overwrite").options(options).csv("/xcn/output/yelp/"+"3")

  println("----------------------4.商家数量最多的10个城市========>")
  // 4.商家数量最多的10个城市
  val bus_city = ss.sql("select city,count(distinct business_id) as busOfNum from part_business group by city order by busOfNum desc limit 10")
  bus_city.write.mode("overwrite").options(options).csv("/xcn/output/yelp/"+"4")

  println("----------------------5.消费者评价最多的10种商业类别========>")
  // 5.消费者评价最多的10种商业类别
  val sum_cat = ss.sql("select category,count(review_count) as avg_review_count from part_business group by category order by avg_review_count desc limit 10 ")
  sum_cat.write.mode("overwrite").options(options).csv("/xcn/output/yelp/"+"5")

  println("----------------------6.最受消费者喜欢的前10种商业类型========>")
  // 6.最受消费者喜欢的前10种商业类型
  val sum_stars = ss.sql("select category,AVG(stars) as avgOfStars from part_business group by category order by avgOfStars desc limit 10 ")
  sum_stars.write.mode("overwrite").options(options).csv("/xcn/output/yelp/"+"6")

  println("----------------------7.商业额外业务的评价情况========>")
  // 7.商业额外业务的评价情况
  val for_att = ss.sql("select attributes,stars,explode(categories) as category from business")
  for_att.createOrReplaceTempView("for_att")
  val att = ss.sql("select attributes as RestaurantsTakeOut, avg(stars) as stars from for_att group by attributes order by stars")
  att.repartition(1).write.mode("overwrite").options(options).csv("/xcn/output/yelp/"+"7")


 }
}
