package cn.spark.study.core

import org.apache.spark.{SparkConf, SparkContext}

object ActionOperation {
  def main(args: Array[String]): Unit = {

  }

  def reduce(): Unit = {
    val conf = new SparkConf()
      .setAppName("reduce")
      .setAppName("local")

    val sc = new SparkContext(conf)

    val numberArray = Array(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)
    val number = sc.parallelize(numberArray, 1)
    val sum = number.reduce(_ + _)
    println(sum)
  }

  def collect(): Unit = {
    val conf = new SparkConf()
      .setAppName("collect")
      .setAppName("local")

    val sc = new SparkContext(conf)

    val numberArray = Array(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)
    val number = sc.parallelize(numberArray, 1)
    val doubleNumbers = number.map(num => num * 2)
    val doubleNumbersArray = doubleNumbers.collect()
    for (num <- doubleNumbersArray) {
      println(num)
    }
  }


  def count(): Unit = {
    val conf = new SparkConf()
      .setAppName("count")
      .setAppName("local")

    val sc = new SparkContext(conf)

    val numberArray = Array(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)
    val number = sc.parallelize(numberArray, 1)
    val count = number.count()
    println(count)
  }


  def take(): Unit = {
    val conf = new SparkConf()
      .setAppName("take")
      .setAppName("local")

    val sc = new SparkContext(conf)

    val numberArray = Array(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)
    val number = sc.parallelize(numberArray, 1)
    val top3Numbers = number.take(3)
    println(top3Numbers)
  }


  def saveAsTextFile(): Unit = {
    val conf = new SparkConf()
      .setAppName("saveAsTextFile")
      .setAppName("local")

    val sc = new SparkContext(conf)
    val numberArray = Array(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)
    val number = sc.parallelize(numberArray, 1)
    number.saveAsTextFile("")
  }



  def countByKey(): Unit = {
    val conf = new SparkConf()
      .setAppName("countByKey")
      .setAppName("local")

    val sc = new SparkContext(conf)
    val scoreList = Array(Tuple2("class1", "leo"),
      Tuple2("class2", "jack"),
      Tuple2("class1", "tom"),
      Tuple2("class2", "jen"),
      Tuple2("class2", "marry"))
    val students = sc.parallelize(scoreList,1)
    val studentCounts = students.countByKey()
    println(studentCounts)


  }


}