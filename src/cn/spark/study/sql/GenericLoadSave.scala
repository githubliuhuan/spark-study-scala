package cn.spark.study.sql

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

object GenericLoadSave {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setAppName("GenericLoadSave")
    val sc = new SparkContext(conf)

    val sqlcontext = new SQLContext(sc)

    val usersDF = sqlcontext.read.load("")
    usersDF.write.save("")


  }
}
