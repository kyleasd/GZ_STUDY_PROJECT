package com.xcn.t2.spark

import java.util
import java.util.regex.Pattern

import org.apache.spark.{SparkConf, SparkContext}

object RecruitTask3 {
  // 实验任务三：将日期字段中的日期格式统一为yyyy-mm-dd
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("RecruitTask3").setMaster("spark://master:7077")
    val sc = new SparkContext(conf)

    val accum = sc.longAccumulator("RecruitSpark")

    val timemap = new util.HashMap[String,String]()
    timemap.put("Jan","01")
    timemap.put("Feb","02")
    timemap.put("Mar","03")
    timemap.put("Apr","04")
    timemap.put("May","05")
    timemap.put("Jun","06")
    timemap.put("Jul","07")
    timemap.put("Aug","08")
    timemap.put("Sept","09")
    timemap.put("Oct","10")
    timemap.put("Nov","11")
    timemap.put("Dec","12")

    // 1 查看原始数据集，发现有六种不合规的数据格式，编译正则表达式，提取里面的年月日

    // Dec 29 2008 11:45 PM
    //val partition1 = "([A-Za-z]{3,4}) ([0-9]{2}) ([0-9]{4}) [0-9]{1,2}:[0-9]{1,2} [A-Za-z]{2}"
    val partition1 = "([a-zA-Z]{3,4}) ([0-9]{2}) ([0-9]{4}) [0-9]{1,2}:[0-9]{1,2} [a-zA-Z]{2}"

    // 12-29-2008
    //val partition2 = "([0-9]{1,2})-([0-9]{1,2})-([0-9]{4})"
    val partition2 = "([0-9]{2})-([0-9]{2})-([0-9]{4})"

    // 29 Dec 2008 16:25:46
    //val partition3 = "([0-9]{1,2}) ([A-Za-z]{3,4}) ([0-9]{4}) [0-9]{1,2}:[0-9]{1,2}:[0-9]{1,2}"
    val partition3 = "([0-9]{2}) ([a-zA-Z]{3,4}) ([0-9]{4}) [0-9]{1,2}:[0-9]{1,2}:[0-9]{1,2}"

    // 18th,Apr,2019,10:52:53:000000
    //val partition4 = "([0-9]{1,2})[A-Za-z]{2},([A-Za-z]{3,4}),([0-9]{4}),[0-9]{1,2}:[0-9]{1,2}:[0-9]{1,2}:[0-9]{6}"
    val partition4 = "([0-9]{2})[a-z]{2},([a-zA-Z]{3,4}),([0-9]{4}),[0-9]{1,2}:[0-9]{1,2}:[0-9]{1,2}:[0-9]{6}"

    // Apr,18th,2019,10:46 AM
    //val partition5 = "([A-Za-z]{3,4}),([0-9]{1,2})[A-Za-z]{2},([0-9]{4}),[0-9]{1,2}:[0-9]{1,2} [A-Z]{2}"
    val partition5 = "([a-zA-Z]{3,4}),([0-9]{2})[a-z]{2},([0-9]{4}),[0-9]{1,2}:[0-9]{1,2} [a-zA-Z]{2}"

    // 18th,Apr,2019 10:52:53
    //val partition6 = "([0-9]{2})[A-Za-z]{2},([A-Za-z]{3,4}),([0-9]{4}) [0-9]{1,2}:[0-9]{1,2}:[0-9]{1,2}"
    val partition6 = "([0-9]{2})[a-z]{2},([a-zA-Z]{3,4}),([0-9]{4}) [0-9]{1,2}:[0-9]{1,2}:[0-9]{1,2}"

    val r1 = Pattern.compile(partition1)
    val r2 = Pattern.compile(partition2)
    val r3 = Pattern.compile(partition3)
    val r4 = Pattern.compile(partition4)
    val r5 = Pattern.compile(partition5)
    val r6 = Pattern.compile(partition6)

    // 2 对输入的每一行数据按|进行切割
    val rdd = sc.textFile("hdfs://master:9000/xcn/output/recruitspark2/")
    rdd.map( x => {
      x.split("\\|")
    })
    // 3 使用正则表达式对日期字段进行匹配，提取年月日，并修改为yyyy-mm-dd的格式
      //  基础架构研发工程师 - 视频架构
      // |滴滴出行
      // |北京
      // |职位描述:1、负责今日头条视频相关基础架构，包括不限于上传、存储、播放、云平台、Serverless/FaaS计算平台等工程架构服务；2、设计、开发支持全球化多IDC、边缘网络的基础架构服务；3、主动发现现有系统的弱点并加以完善，确保模块线上运行稳定 ；4、对业务逻辑进行合理抽象，高效地满足架构、业务需求；职位要求:1、至少2年服务端或多媒体处理开发经验，强悍的系统设计及编码能力 ；2、精通主流语言的至少一门 C/C++/Java/Python/PHP/Go/Erlang 等 ；3、有优秀的逻辑分析能力，能够对业务逻辑进行合理的抽象和拆分 ；4、积极乐观，责任心强，工作认真细致，具有良好的团队沟通与协作能力；5、有强烈的求知欲、好奇心和进取心 ，能及时关注和学习业界***；其他加分项：1、有大规模分布式对象、文件存储经验优先；2、有国内外知名云计算团队的工作经历优先；3、有docker、kvm、mesos、kubernetes、swarm等开源项目经验优先；4、熟悉AWS lambda、Google Function、Azure Function等产品的优先；职能类别：系统架构设计师微信分享
      // |14
      // |10000-6000
      // |Hadoop, Hive, Pig, Sqoop, Flume, Python
      // |2019-03-12
      // |男
      // |滴滴出行是涵盖出租车、专车、 快车、顺风车、代驾及 大巴等多项业务在内的一站式出行平台，2015年9月9日由“滴滴打车”更名而来
      // |本科及其以上
      .map( x => {
        var m = r1.matcher(x(7))

//        if( m.find() ){
//          accum.add(1)
//          x(7) = m.group(3) + "-" + m.group(2) + "-" + timemap.get(m.group(1))
//
//        }else{
//          m = r2.matcher(x(7))
//          if( m.find() ){
//            accum.add(1)
//            x(7) = m.group(3) + "-" + m.group(2) + "-" + m.group(1)
//
//          }else{
//            m = r3.matcher(x(7))
//            if( m.find() ){
//              accum.add(1)
//              x(7) = m.group(3) + "-" + timemap.get(m.group(2)) + "-" + m.group(1)
//
//            }else{
//              m = r4.matcher(x(7))
//              if( m.find() ){
//                accum.add(1)
//                x(7) = m.group(3) + "-" + timemap.get(m.group(2)) + "-" + m.group(1)
//              }else{
//                m = r5.matcher(x(7))
//                if( m.find() ){
//                  accum.add(1)
//                  x(7) = m.group(3) + "-" +timemap.get(m.group(1)) + "-" + m.group(2)
//                }else{
//                  m = r6.matcher(x(7))
//                  if( m.find() ){
//                    accum.add(1)
//                    x(7) = m.group(3) + "-" + timemap.get(m.group(2)) + "-" + m.group(1)
//                  }
//
//                }
//
//              }
//            }
//
//          }
//        }


        x
      }).map( x => {
      var result = ""
      for( y <- x ){
        if( y.equals(x(10)) ){
          result += y.trim
        }else{
          result += y.trim + "|"
        }
      }
      result
    }).repartition(1).saveAsTextFile("hdfs://master:9000/xcn/output/recruitspark3")
      println("----------修改记录数为："+accum.value+"--------------------")




  }

}
