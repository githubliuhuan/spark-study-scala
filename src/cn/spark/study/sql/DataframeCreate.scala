package cn.spark.study.sql

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

object DataframeCreate {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setAppName("DataframeCreate")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    // 创建出来的DataFrame完全可以理解为一张表
    val df = sqlContext.read.json("")
    // 打印DataFrame所有数据（select * from ...)
    df.show()
    // 打印DataFrame的元数据（Schema）
    df.printSchema()
    // 查询某列所有的数据
    df.select("name").show()
    // 查询某几列所有的数据，并对列进行计算
    df.select(df("name"),df("age") + 1).show()
    // 根据某一列的值进行过滤
    df.filter(df("age") > 18).show()
    // 根据某一列进行分组，然后进行聚合
    df.groupBy(df("age")).count().show()

  }
}
