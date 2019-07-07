package cn.spark.study.sql

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.hive.HiveContext

object RowNumberWindowFunction {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setAppName("RowNumberWindowFunction")
      .setMaster("local")
    val sc = new SparkContext(conf)
    val hivecontext = new HiveContext(sc)
    hivecontext.sql("drop table if exists sales")
    hivecontext.sql("create table if not exists sales (" +
      "product string," +
      "category string," +
      "revenue bigint)")
    hivecontext.sql("load data" +
      "local inpath '/usr/local/spark-study/resources/sales.txt'" +
      "into table sales")

    val top3SalesDF = hivecontext.sql("" +
      "select product,category,revenue" +
      "from (select product,category,revenue," +
      "row_number() over (partition by category order by  revenue desc) rank" +
      "from sales) tmp" +
      "where rank <= 3")
    top3SalesDF.write.saveAsTable("top3_sales")




  }
}
