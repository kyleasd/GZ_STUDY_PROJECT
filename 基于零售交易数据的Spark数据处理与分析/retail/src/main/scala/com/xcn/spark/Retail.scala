package com.xcn.spark


import org.apache.spark.sql.types.{FloatType, IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SparkSession}


object Retail {
  def main(args: Array[String]): Unit = {

    val ss = SparkSession.builder().getOrCreate()

    val options = Map("header"->"true","inferschema"->"true")
    val output = "hdfs://master:9000/xcn/output/retail/"

    println("----------------------------"+"正在初始化数据"+"----------------------------")
    val df = ss.read.options(options).csv("hdfs://master:9000/xcn/input/E_Commerce_Data.csv")
    df.createOrReplaceTempView("data")
    ss.sql("select * from data where CustomerID !=0 and Description != '' ").createOrReplaceTempView("data")
    println("----------------------------"+"正在处理第一条问题"+"----------------------------")
    //1.客户数最多的10个国家
    val countryCustomer = ss.sql("select Country,count(distinct CustomerID) from data group by country")
    countryCustomer.repartition(1).write.options(options).csv(output+"1")
    println("----------------------------"+"正在处理第二条问题"+"----------------------------")
    //2.销量最高的10个国家
    val countryQuantity = ss.sql("select Country,count(distinct StockCode) from data group by country")
    countryQuantity.repartition(1).write.options(options).csv(output+"2")
    println("----------------------------"+"正在处理第三条问题"+"----------------------------")
    //3.各个国家的总销售额分布情况
    val countryOfSales = ss.sql("select Country,sum(UnitPrice*Quantity) from data group by country")
    countryOfSales.repartition(1).write.options(options).csv(output+"3")
    println("----------------------------"+"正在处理第四条问题"+"----------------------------")
    //4.销量最高的10个商品
    val stockQuantity = ss.sql("select StockCode,sum(Quantity) as Quantity from data group by StockCode order by Quantity desc limit 10")
    stockQuantity.repartition(1).write.options(options).csv(output+"4")
    println("----------------------------"+"正在处理第五条问题"+"----------------------------")

    //5.商品描述的热门关键词Top300
    val wordCount = ss.sql("select lower(Description) from data").rdd.flatMap( _(0).toString.split(" "))
      .map( (_,1) ).reduceByKey(_+_).repartition(1).sortBy(_._2,false).map( x => Row(x._1,x._2))
    val wordCountSchema = StructType(Array(StructField("word",StringType,true),StructField("count",IntegerType,true)))

    ss.createDataFrame(wordCount,wordCountSchema).createOrReplaceTempView("wordCountDF")
    ss.sql("select * from wordCountDF where word != '' limit 300").repartition(1).write.options(options).csv(output+"5")

    println("----------------------------"+"正在处理第六条问题"+"----------------------------")
    //6.退货订单数最多的10个国家
    val countryReturnInvoiceDF = ss.sql("select Country,count(distinct InvoiceNo) as countOfReturnInvoice from data where InvoiceNo like 'C%' group by Country order by countOfReturnInvoice limit 10")
    countryReturnInvoiceDF.repartition(1).write.options(options).csv(output+"6")

    println("----------------------------"+"正在处理第七条问题"+"----------------------------")
    //7.月销售额随时间的变化趋势
    val tradeRDD = ss.sql("select InvoiceDate, UnitPrice, Quantity from data").rdd.map( x => (x(0).toString,x(1).toString,x(2).toString) )
    val result = tradeRDD.map( x => (x._1.split(" ")(0).split("/"),x._2,x._3) )
    val result2 = result.map( x => {
      if( x._1(0).length == 2 ){
        (x._1(2)+"-"+x._1(0),x._2.toFloat*x._3.toFloat)
      }else{
        (x._1(2)+"-0"+x._1(0),x._2.toFloat*x._3.toFloat)
      }
    }).reduceByKey(_+_).sortByKey().map( x => Row(x._1,x._2) )

    val tradeSchema = StructType(Array(StructField("Date",StringType,true),StructField("tradePrice",FloatType,true)))
    val tradeDF = ss.createDataFrame(result2,tradeSchema)

    tradeDF.repartition(1).write.options(options).csv(output+"7")


    println("----------------------------"+"正在处理第八条问题"+"----------------------------")
    //8.日销量随时间的变化趋势
    val dayTradeRdd = result.map( x => {
      var key = ""
      val value = x._2.toFloat*x._3.toFloat
      var month = ""
      var day = ""

      if( x._1(0).length == 2 ){

        month = "-"+ x._1(0)


      }else{

        month = "-0"+x._1(0)

      }

      if( x._1(1).length == 2 ){
        day = "-" + x._1(1)
      }else{
        day = "-0" + x._1(1)
      }
      key = x._1(2) + month + day
      (key,value)
    }).reduceByKey(_+_).sortByKey().map( x => Row(x._1, x._2) )
    val dayTradeDF = ss.createDataFrame(dayTradeRdd,tradeSchema)
    dayTradeDF.repartition(1).write.options(options).csv(output+"8")

    //InvoiceNo	StockCode	 Description
    //536365	  85123A	     WHITE HANGING HEART T-LIGHT HOLDER

    //Quantity	InvoiceDate	    UnitPrice	CustomerID	Country
    //6	        12/1/2010 8:26	2.55	    17850	      United Kingdom


    println("----------------------------"+"正在处理第九条问题"+"----------------------------")
    //9.各国的购买订单量和退货订单量的关系
    val returnDF = ss.sql("select Country,count(distinct InvoiceNo) as countOfReturn from data where InvoiceNo like 'C%' group by Country")
    val buyDF = ss.sql("select Country,count(distinct InvoiceNo) as countOfBuy from data where InvoiceNo not like 'C%' group by Country")
    val buyReturnDF = returnDF.join(buyDF,"Country")

    buyReturnDF.repartition(1).write.options(options).csv(output+"9")


    println("----------------------------"+"正在处理第十条问题"+"----------------------------")
    //10.商品的平均单价与销量的关系
    val unitPriceSalesDF = ss.sql("select StockCode,avg(distinct UnitPrice) as avgUnitPrice,sum(Quantity) from data group by StockCode")

    unitPriceSalesDF.repartition(1).write.options(options).csv("/xcn/output/retail/10")
  }
}
