package cn.spark.study.core

import org.apache.spark.{SparkConf, SparkContext}


object LocalFile {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setAppName("LocalFile")
      .setMaster("Local")
    val sc = new SparkContext(conf)

    val lines = sc.textFile("", 1)
    val count = lines.map(line => line.length()).reduce(_ + _)

    println("file's count is " + count)
  }
}
