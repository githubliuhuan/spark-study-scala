package cn.spark.study.sql

import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}

object DailyUV {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setAppName("DailyUV")
      .setMaster("local")
    val sc = new SparkContext(conf)
    val sqlcontext = new SQLContext(sc)

    // 构造用户访问日志数据，并创建DataFrame

    val userAccessLog = Array(
      "2015-10-01,1122",
      "2015-10-01,1122",
      "2015-10-01,1123",
      "2015-10-01,1124",
      "2015-10-01,1124",
      "2015-10-02,1122",
      "2015-10-02,1121",
      "2015-10-02,1123",
      "2015-10-02,1123",
    )

    val userAccessLogRDD = sc.parallelize(userAccessLog)

    // 将模拟出来的用户访问日志RDD，转换为DataFrame
    // 首先，将普通的RDD，转换为元素为row的RDD

    val userAccessLogRowRDD = userAccessLogRDD
      .map{log => Row(log.split(","), log.split(",")(1).toInt)}

    // 构造DataFrame的元数据
    val structType = StructType(Array(
      StructField("date", StringType),
      StructField("userid", IntegerType)))

    // 使用SQLcontext创建Dataframe
    val userAccessLogRowDF = sqlcontext.createDataFrame(userAccessLogRowRDD, structType)

    // 首先，对DataFrame调用groupBy()方法，对某一列进行分组

    // 然后调用agg()方法，第一个参数必须传入之前在groupBy()方法中出现的字段
    // 第二个参数，传入countDistinct、sum、first等，Spark提供的内置函数
    // 内置函数中，传入的参数，也是用单引号作为前缀的，其他的字段
    // 要使用spark sql的内置函数，必须导入SQLcontext下的隐式转换
    import sqlcontext.implicits._


    userAccessLogRowDF.groupBy("date")
      .agg('date, countDistinct('userid))
      .map{ row => Row(row(1), row(2))}
      .collect()
      .foreach(println)
  }
}
