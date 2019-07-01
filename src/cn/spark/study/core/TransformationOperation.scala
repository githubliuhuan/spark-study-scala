package cn.spark.study.core

import org.apache.spark.{SparkConf, SparkContext}


// @author liuhuan
object TransformationOperation {
  def main(args: Array[String]): Unit = {
    // map()
    // filter()
    flatMap()
  }

  def map(): Unit = {
    val conf = new SparkConf()
      .setAppName("LineCount")
      .setMaster("local")
    val sc = new SparkContext(conf)

    val numbers = Array(1, 2, 3, 4, 5, 6, 7, 8, 9)
    val numberRDD = sc.parallelize(numbers,1)
    val multipleNumberRDD = numberRDD.map(num => num * 2)

    multipleNumberRDD.foreach(num => println(num))
  }


  def filter(): Unit = {
    val conf = new SparkConf()
      .setAppName("LineCount")
      .setMaster("local")
    val sc = new SparkContext(conf)

    val numbers = Array(1, 2, 3, 4, 5, 6, 7, 8, 9)
    val numberRDD = sc.parallelize(numbers,1)
//    def even(e: Int): Boolean = {
//      e % 2 == 0
//    }
    val evenNumberRDD = numberRDD.filter(number => number % 2 == 0)
    evenNumberRDD.foreach(num => println(num))
  }


  def flatMap(): Unit = {
    val conf = new SparkConf()
      .setAppName("flatMap")
      .setMaster("local")

    val sc = new SparkContext(conf)

    val lineArray = Array("hello you", "hello me", "hello world")
    val linesRDD = sc.parallelize(lineArray)
    val wordsRDD = linesRDD.flatMap(line => line.split(" "))
    wordsRDD.foreach(word => println(word))
  }


  def groupByKey(): Unit = {
    val conf = new SparkConf()
      .setAppName("groupByKey")
      .setAppName("local")

    val sc = new SparkContext(conf)
    val scoreList = Array(Tuple2("class1", 80), Tuple2("class2", 75),
      Tuple2("class1", 90), Tuple2("class2", 60))
    val scoreRDD = sc.parallelize(scoreList)

    val groupedScore = scoreRDD.groupByKey()

    groupedScore.foreach{
      score => {
        println(score._1)
        score._2.foreach{
          singleScore => println(singleScore)
            println("==========================")
        }
      }
    }
  }


  def reduceByKey(): Unit = {
    val conf = new SparkConf()
      .setAppName("groupByKey")
      .setAppName("local")

    val sc = new SparkContext(conf)
    val scoreList = Array(Tuple2("class1", 80), Tuple2("class2", 75),
      Tuple2("class1", 90), Tuple2("class2", 60))
    val scoreRDD = sc.parallelize(scoreList)

    val totalScore = scoreRDD.reduceByKey(_ + _)

    totalScore.foreach(classScore => println(classScore._1 + ": " + classScore._2))
}


  def sortByKey(): Unit = {
    val conf = new SparkConf()
      .setAppName("sortByKey")
      .setAppName("local")

    val sc = new SparkContext(conf)
    val scoreList = Array(Tuple2(65, "Leo"), Tuple2(50, "Tom"),
      Tuple2(100, "Marry"), Tuple2(85, "Jack"))
    val scores = sc.parallelize(scoreList, 1)
    val sortedScoresRDD = scores.sortByKey(ascending = false) // 默认升序，false降序
    sortedScoresRDD.foreach(studentScore => println(studentScore._1 + ": " + studentScore._2))

  }


  def join(): Unit = {
    val conf = new SparkConf()
      .setAppName("join")
      .setAppName("local")

    val sc = new SparkContext(conf)

    val studentList = Array(
      Tuple2(1, "leo"),
      Tuple2(2, "jack"),
      Tuple2(3, "tom")
    )

    val scoreList = Array(
      Tuple2(1, 100),
      Tuple2(2, 90),
      Tuple2(3, 60)
    )

    val studentRDD = sc.parallelize(studentList)
    val scoreRDD = sc.parallelize(scoreList)

    val studentScoreRDD = studentRDD.join(scoreRDD)

    studentScoreRDD.foreach{studentScore => {
      println("student id：" + studentScore._1)
      println("student name: " + studentScore._2._1)
      println("student score: " + studentScore._2._2)
      println("===========================")
    }

    }

    }





}