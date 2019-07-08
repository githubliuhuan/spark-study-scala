package cn.spark.caifu

import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

object YiXinCheckCount {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setAppName("YiXinCheckCount")

    val sc = new SparkContext(conf)

    val hivecontext = new HiveContext(sc)
    val checkTableDF = hivecontext.sql("select 'dm.dim_crm_customer' as table_name,count(*),from_unixtime(unix_timestamp(),'yyyy-MM-dd HH:mm:ss') as check_time from dm.dim_crm_customer")
    checkTableDF.write.mode(SaveMode.Append).format("hive").saveAsTable("dm.table_count")
    println(checkTableDF.collect().foreach(println))

  }
}
