package cn.spark.study.sql

import org.apache.spark.sql.{SQLContext, SaveMode}
import org.apache.spark.{SparkConf, SparkContext}


// SaveMode.ErrorifExists  如果目标位置存在跑出异常
// SaveMode.Append 追加
// SaveMode.Overwrite 覆盖
// SaveMode.ignore 忽略

object SaveModeTest {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setAppName("SaveModeTest")
      .setMaster("local")

    val sc = new SparkContext(conf)

    val sqlcontext = new SQLContext(sc)

    val peopleDF = sqlcontext.read.format("json")
      .load("hdfs://spark1:9000/people.json")
    peopleDF.save("hdfs://spark1:9000/people_savemode_test","json", SaveMode.ErrorIfExists)

  }
}
