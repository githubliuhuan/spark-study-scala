package cn.spark.study.sql

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext

object ParquetLoadData {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setAppName("ParquetLoadData")
      .setMaster("local")

    val sc = new SparkContext(conf)

    val sqlcontext = new SQLContext(sc)

    val usersDF = sqlcontext.read.parquet(".parquet")
    usersDF.registerTempTable("users")
    val userNamesDF = sqlcontext.sql("select name from users")
    userNamesDF.rdd.map{
      row => "Name: " + row(0)
    }.collect()
      .foreach(userName => println(userName))

  }
}
