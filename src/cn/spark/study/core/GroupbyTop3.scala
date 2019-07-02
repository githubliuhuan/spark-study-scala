package cn.spark.study.core

import org.apache.spark.{SparkConf, SparkContext}

object GroupbyTop3 {
  def main(args: Array[String]): Unit = {
    val conf=new SparkConf()
      .setAppName("GroupTopn")
      .setMaster("local")
    val sc=new SparkContext(conf)

    //读取HDFS中的数据
    val lines=sc.textFile("hdfs://tgmaster:9000/in/classinfo")

    /**
      * 1、通过map算子形成映射(class,score)
      * 2、通过groupByKey算子针对班级Key进行分组
      * 3、通过map算子对分组之后的数据取前3名，核心代码：val top3=m._2.toArray.sortWith(_>_).take(3)
      * 4、通过foreach算子遍历输出
      */
    lines.map(m=>{
      val info=m.split(" ")
      (info(0),info(1).toInt)})
      .groupByKey().map(m=>{
      val className=m._1
      val top3=m._2.toArray.sortWith(_>_).take(3)
      (className,top3)
    }).foreach(item=>{
      val className=item._1
      println("班级："+className+"的前3名是：")
      item._2.foreach(m=>{
        println(m)
      })
    })
  }
}
