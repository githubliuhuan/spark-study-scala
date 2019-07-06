package cn.spark.study.sql

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

object DataframeCreate {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setAppName("DataframeCreate")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    val df = sqlContext.read.json("")
    df.show()

  }
}
