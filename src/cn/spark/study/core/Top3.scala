package cn.spark.study.core

import org.apache.spark.{SparkConf, SparkContext}

object Top3 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setAppName("Top3")
      .setMaster("local")

    val sc = new SparkContext(conf)

    val lines = sc.textFile("", 1)
    val pairs = lines.map(line => (line.toInt, line))
    val sortedPairs = pairs.sortByKey(ascending = false)
    val sortedNumbers = sortedPairs.map(sortedPair => sortedPair._1)
    val top3Number = sortedNumbers.take(3)

    for (num <- top3Number) {
      println(num)
    }

  }
}
