package cn.spark.study.sql

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext

object ManuallySpecifyOptions {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setAppName("ManuallySpecifyOptions")
    val sc = new SparkContext(conf)

    val sqlcontext = new SQLContext(sc)

    val peopleDF = sqlcontext.read.format("json").load("hdfs://spark1:9000/people.json")
    peopleDF.select("name").write.format("parquet").save("hdfs://spark1:9000/peopleName_scala")

  }
}
