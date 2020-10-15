package com.xcn.spark

import org.apache.spark.sql.SparkSession


object virus {
  def main(args: Array[String]): Unit = {


    //  date	county	state	cases	deaths
    //  2020/1/21	Snohomish	Washington	1	0

    val ss = SparkSession.builder().appName("virus").enableHiveSupport().getOrCreate()
    import ss.implicits._

    // 加载初始数据，按逗号切割
    val rdd = ss.sparkContext.textFile("hdfs://master:9000/xcn/input/us-counties.csv").filter( x => !x.contains("date") ).map( _.split(",")).repartition(1)

    val viruses = rdd.map( x => { virus2(toDate(x(0)),x(1),x(2),x(3).toInt,x(4).toInt)}).toDF()
    viruses.createOrReplaceTempView("viruses")

    // 1.统计美国截止每日的累计确诊人数和累计死亡人数。做法是以date作为分组字段，对cases和deaths字段进行汇总统计。
    val usAll = ss.sql("select date,sum(cases) as cases,sum(deaths) as deaths from viruses group by date")
    usAll.createOrReplaceTempView("usAll")
    usAll.repartition(1).write.json("hdfs://master:9000/xcn/output/virus/job1")

    // 2.统计美国每日的新增确诊人数和新增死亡人数。因为新增数=今日数-昨日数，所以考虑使用自连接，连接条件是t1.date = t2.date + 1，然后使用t1.totalCases – t2.totalCases计算该日新增
     ss.sql("select t1.date,t1.cases-t2.cases as casesIncrease,t1.deaths-t2.deaths as deathIncrease from usAll t1,usAll t2 where t1.date = date_add(t2.date,1) ").repartition(1).write.json("hdfs://master:9000/xcn/output/virus/job2")
    //ss.sql("select t1.date,t1.cases-t2.cases as caseIncrease,t1.deaths-t2.deaths as deathIncrease from ustotal t1,ustotal t2 where t1.date = date_add(t2.date,1)")


    // 3.统计截止5.19日，美国各州的累计确诊人数和死亡人数。首先筛选出5.19日的数据，然后以state作为分组字段，对cases和deaths字段进行汇总统计。
    val ustotal = ss.sql("select date, state,sum(cases) as cases, sum(deaths) as deaths from viruses where date = '2020-05-19' group by date,state")
    ustotal.createOrReplaceTempView("ustotal")
    ustotal.write.json("hdfs://master:9000/xcn/output/virus/job3")


    // 4.统计截止5.19日，美国确诊人数最多的十个州。对3)的结果DataFrame注册临时表，然后按确诊人数降序排列，并取前10个州。
    ss.sql("select state,cases from ustotal order by cases desc limit 10").write.json("hdfs://master:9000/xcn/output/virus/job4")

    // 5.统计截止5.19日，美国死亡人数最多的十个州。对3)的结果DataFrame注册临时表，然后按死亡人数降序排列，并取前10个州。
    ss.sql("select state,deaths from ustotal order by deaths desc limit 10 ").write.json("hdfs://master:9000/xcn/output/virus/job5")

    // 6.统计截止5.19日，美国确诊人数最少的十个州。对3)的结果DataFrame注册临时表，然后按确诊人数升序排列，并取前10个州。
    ss.sql("select state,cases from ustotal order by cases limit 10").write.json("hdfs://master:9000/xcn/output/virus/job6")

    // 7.统计截止5.19日，美国死亡人数最少的十个州。对3)的结果DataFrame注册临时表，然后按死亡人数升序排列，并取前10个州
    ss.sql("select state,deaths from ustotal order by deaths limit 10").write.json("hdfs://master:9000/xcn/output/virus/job7")

    // 8.统计截止5.19日，全美和各州的病死率。病死率 = 死亡数/确诊数，对3)的结果DataFrame注册临时表，然后按公式计算。
    ss.sql("select state,deaths/cases from ustotal ").write.json("hdfs://master:9000/xcn/output/virus/job8")

  }

  //  2020/1/21 -> 2020-01-21
  def toDate(init:String):String = {

    var result = ""
    if( init.length == 8 ){
      val da1 = init.substring(0,4)
      val da2 = init.substring(5,6)
      val da3 = init.substring(7)
      result = da1 + "-" + "0" + da2 + "-" + "0" + da3
    } else {
      val da1 = init.substring(0,4)
      val da2 = init.substring(5,6)
      val da3 = init.substring(7,init.length)
      result = da1 + "-" + "0" + da2 + "-" + da3
    }

    result
  }
  case class virus2(date:String,county:String,state:String,cases:Int,deaths:Int)
}


