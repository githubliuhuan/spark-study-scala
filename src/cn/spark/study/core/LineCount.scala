package cn.spark.study.core

import org.apache.spark.{SparkConf, SparkContext}

object LineCount {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setAppName("LineCount")
      .setMaster("local")

    val sc = new SparkContext(conf)

    val lines = sc.textFile("",1)
    val pairs = lines.map(line => (line, 1))
    val lineCounts = pairs.reduceByKey(_ + _)

    lineCounts.foreach(lineCount => println(lineCount._1 + "appears" + lineCount._2 + "times."))
  }
}
