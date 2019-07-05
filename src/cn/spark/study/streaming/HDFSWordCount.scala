package cn.spark.study.streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

object HDFSWordCount {

  object WordCount {
    def main(args: Array[String]): Unit = {
      val conf = new SparkConf()
        .setMaster("lcoal[2]")
        .setAppName("HDFSWordCount")

      val ssc = new StreamingContext(conf, Seconds(5))

      val lines = ssc.textFileStream("")

      val words = lines.flatMap(_.split(" "))

      val pairs = words.map { word => (word, 1) }

      val wordCounts = pairs.reduceByKey(_ + _)

      Thread.sleep(5000)

      wordCounts.print()
    }
  }

}