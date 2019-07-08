package cn.spark.study.sql

import org.apache.spark.sql.SaveMode
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.hive.HiveContext

object YiXinCheckCount {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setAppName("YiXinCheckCount")

    val sc = new SparkContext(conf)

    val hivecontext = new HiveContext(sc)
    val checkTableDF = hivecontext.sql("select count(*),unix_timestamp(from_unixtime("YYYY-MM-dd HH:mm:ss")) from dm.dim_crm_customer")
    println(checkTableDF)
    checkTableDF.write.mode(SaveMode.Append).saveAsTable("")
}
}
