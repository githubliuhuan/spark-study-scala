package cn.spark.study.sql

import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}

object UDF {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setMaster("local")
      .setAppName("UDF")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    // 构造模拟数据
    val names = Array("Loo", "Jack", "Tom")
    val namesRDD = sc.parallelize(names, 5)
    val namesRowRDD = namesRDD.map( name => Row(name))
    val structType = StructType(Array(StructField("name", StringType)))
    val namesDF = sqlContext.createDataFrame(namesRowRDD, structType)

    // 注册一张临时表
    namesDF.registerTempTable("names")

    // 定义和注册自定义函数
    // 定义函数，自己写匿名函数
    // 注册函数，SQLcontext.udf.register()
    sqlContext.udf.register("strLen", (str: String) => str.length)

    // 使用自定义函数
    sqlContext.sql("select name,strLen(name) from names")
      .collect()
      .foreach(println)
  }

}
