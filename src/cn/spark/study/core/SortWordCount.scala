package cn.spark.study.core

import org.apache.spark.{SparkConf, SparkContext}

object SortWordCount {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setAppName("SortWordCount")
      .setMaster("local")
    val sc = new SparkContext(conf)

    val lines = sc.textFile("", 1)
    val linesRDD = lines.flatMap(line => line.split(" "))

    val wordsRDD = linesRDD.map(line => (line, 1))

    val reduceWordsRDD = wordsRDD.reduceByKey(_ + _)

    val reserveWordsRDD = reduceWordsRDD.map(word => (word._2, word._1))

    val sortedWordsRDD = reserveWordsRDD.sortByKey(ascending = false)

    val wordsReserveRDD = sortedWordsRDD.map(word => (word._2, word._1))

    wordsReserveRDD.foreach(word => println(word._1 + "appears:" + word._2))

  }
}
