package cn.spark.study.core

import org.apache.spark.{SparkConf, SparkContext}

object HDFSFile {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setAppName("HDFSFile")

    val sc = new SparkContext(conf)

    val lines = sc.textFile("hdfs://nebula/user/hive/warehouse/dm/weibo_hufen", 1)

    val count = lines.map(line => line.length()).reduce(_ + _)
    println("file's count is " + count)

  }
}
