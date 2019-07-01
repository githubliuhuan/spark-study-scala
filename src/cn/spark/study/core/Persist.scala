package cn.spark.study.core

import org.apache.spark.{SparkConf, SparkContext}

object Persist {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setAppName("Persist")
      .setAppName("local")

    val sc = new SparkContext(conf)
    val lines = sc.textFile("")
      // .cache()
      .persist()

    val count = lines.count()
    println(count)

    val count1 = lines.count()
    println(count1)

  }
}
