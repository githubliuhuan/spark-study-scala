package cn.spark.study.sql

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.sql.types.{DoubleType, StringType, StructField, StructType}
import org.apache.spark.sql.functions._

object DailySale {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setAppName("DailySale")
      .setMaster("local")
    val sc = new SparkContext(conf)
    val sqlcontext = new SQLContext(sc)

    // 构造用户访问日志数据，并创建DataFrame

    val userSaleLog = Array(
      "2015-10-01,55.05,1122",
      "2015-10-01,23.15,1133",
      "2015-10-01,15.20,",
      "2015-10-02,56.05,1144",
      "2015-10-02,78.87,1155",
      "2015-10-02,113.02,1123"
    )

    val userSaleLogRDD = sc.parallelize(userSaleLog)

    // 进行有效日志的过滤
    val filteredUserSaleLogRDD = userSaleLogRDD.filter{ log => if (log.split(",").length == 3 ) true else false}

    // 将模拟出来的用户访问日志RDD，转换为DataFrame
    // 首先，将普通的RDD，转换为元素为row的RDD

    val userSaleLogRowRDD = filteredUserSaleLogRDD
      .map{log => Row(log.split(","), log.split(",")(1).toDouble, log.split(",")(2).toInt)}

    // 构造DataFrame的元数据
    val structType = StructType(Array(
      StructField("date", StringType),
      StructField("sale_amount", DoubleType)))

    // 使用SQLcontext创建Dataframe
    val userSaleLogRowDF = sqlcontext.createDataFrame(userSaleLogRowRDD, structType)

    // 首先，对DataFrame调用groupBy()方法，对某一列进行分组

    // 然后调用agg()方法，第一个参数必须传入之前在groupBy()方法中出现的字段
    // 第二个参数，传入countDistinct、sum、first等，Spark提供的内置函数
    // 内置函数中，传入的参数，也是用单引号作为前缀的，其他的字段
    // 要使用spark sql的内置函数，必须导入SQLcontext下的隐式转换
    import sqlcontext.implicits._


    userSaleLogRowDF.groupBy("date")
      .agg('date, sum('sale_amount))
      .map{ row => Row(row(1), row(2))}
      .collect()
      .foreach(println)
  }
}
