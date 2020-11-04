package com.xcn.spark

import com.alibaba.fastjson.{JSON, JSONArray}
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types._

object yelp1 {

  //{

  // "business_id":"f9NumwFMBDn751xgFiRbNA","name":"The Range At Lake Norman",
  // "address":"10913 Bailey Rd",
  // "city":"Cornelius","state":"NC",
  // "postal_code":"28031","latitude":35.4627242,
  // "longitude":-80.8526119,"stars":3.5,
  // "review_count":36,"is_open":1,

  // "attributes":
  // {"BusinessAcceptsCreditCards":"True","BikeParking":"True","GoodForKids":"False",
  // "BusinessParking":"
  // {'garage': False, 'street': False,
  // 'validated': False, 'lot': True, 'valet': False
  // }",
  // "ByAppointmentOnly":"False","RestaurantsPriceRange2":"3"
  // },
  // "categories":"Active Life, Gun\/Rifle Ranges, Guns & Ammo, Shopping",


  // "hours":
  // {"Monday":"10:0-18:0","Tuesday":"11:0-20:0","Wednesday":"10:0-18:0","Thursday":"11:0-20:0","Friday":"11:0-20:0","Saturday":"11:0-20:0","Sunday":"13:0-18:0"}

  // }

  def main(args: Array[String]): Unit = {
    val ss = SparkSession.builder().getOrCreate()
    val rdd = ss.sparkContext.textFile("hdfs://master:9000/xcn/input/yelp_academic_dataset_business.json")
    val tmpRdd = rdd.map( x => {

      val JSONObject = JSON.parseObject(x)
      val business_id = JSONObject.getOrDefault("business_id","")
      val name = JSONObject.getOrDefault("name","")
      val address = JSONObject.getOrDefault("address","")

      val city = JSONObject.getOrDefault("city","")

      val state = JSONObject.getOrDefault("state","")
      val postal_code = JSONObject.getOrDefault("postal_code","")
      val latitude = JSONObject.getOrDefault("latitude","")
      val longitude = JSONObject.getOrDefault("longitude","")
      val stars = JSONObject.getOrDefault("stars","")
      val review_count = JSONObject.getOrDefault("review_count","")
      val is_open = JSONObject.getOrDefault("is_open","")

      val attributes = JSONObject.getOrDefault("attributes","")
      var att_take_out:AnyRef = ""
      if( attributes != null ){
         att_take_out = JSON.parseObject(attributes.toString).getOrDefault("RestaurantsTakeOut","none")
      }
      val categories =  JSONObject.getOrDefault("categories","")

      val hours = JSONObject.getOrDefault("hours","")

      Array(business_id,name,address,city,state,postal_code,latitude,longitude,stars,review_count,is_open,categories,att_take_out,hours)
    }).filter( x => {
      var result = true
      for(  i <- 0 to 13 ){
        if ( x(i) == null  ){
          result = false
        }else if( x(i).toString.trim.equals("") ){
          result = false
        }
      }
        result
    }).map( x => Row(x(0).toString.trim,x(1).toString.trim,x(2).toString.trim,x(3).toString.trim,x(4).toString.trim,x(5).toString.trim,x(6).toString.trim.toFloat,x(7).toString.trim.toFloat,x(8).toString.trim,x(9).toString.trim,x(10).toString.trim,x(11).toString.trim.split(","),x(12).toString.trim.toLowerCase(),x(13).toString.trim) )

    val tmpSchema = StructType(Array(StructField("business_id",StringType,true),StructField("name",StringType,true),StructField("address",StringType,true),StructField("city",StringType,true),StructField("state",StringType,true),
      StructField("postal_code",StringType,true),StructField("latitude",FloatType,true),StructField("longitude",FloatType,true),StructField("stars",StringType,true),StructField("review_count",StringType,true),
      StructField("is_open",StringType,true),StructField("categories",ArrayType(StringType,true),true),StructField("attributes",StringType,true),StructField("hours",StringType,true)) )
    val tmpDF = ss.createDataFrame(tmpRdd,tmpSchema)

    tmpDF.createOrReplaceTempView("business")

    val b_etl = ss.sql("SELECT business_id, name, city, state, latitude, longitude, stars, review_count, is_open, categories, attributes FROM business").cache()
    b_etl.createOrReplaceTempView("b_etl")


    val outlier = ss.sql("select b1.business_id,sqrt(power(b1.latitude - b2.avg_lat,2) + power(b1.longitude - b2.avg_long,2)) as dist from b_etl b1 inner join (select state,avg(latitude) as avg_lat,avg(longitude) as avg_long from b_etl group by state) b2 on b1.state = b2.state order by dist desc")
    outlier.createOrReplaceTempView("outlier")
    val joined = ss.sql("select b.* from b_etl b inner join outlier o on b.business_id = o.business_id where o.dist < 10")


    joined.write.mode("overwrite").parquet("hdfs://master:9000/xcn/output/yelp/init")
  }


}
