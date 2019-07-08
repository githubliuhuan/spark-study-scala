package cn.spark.study.sql

import org.apache.spark.sql.types.{IntegerType, StructField, StructType}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{Row, SQLContext}

object RDD2DataframeProgrammatically extends App{
  val conf = new SparkConf()
    .setMaster("local")
    .setAppName("RDD2DataframeProgrammatically")
  val sc = new SparkContext(conf)
  val sqlContext = new SQLContext(sc)


  // 第一步，构造出元素为Row的普通RDD
  val studentRDD = sc.textFile("",1)
    .map{ line => Row(line.split(",")(0).toInt, line.split(",")(1), line.split(",")(2).toInt)}

  // 第二步，编程方式动态构造元数据
  val structType = StructType(Array(
    StructField("id", IntegerType),
    StructField("name", IntegerType),
    StructField("age", IntegerType)
  ))

  // 第三步，进行RDD到DataFrame的转换
  val studentDF = sqlContext.createDataFrame(studentRDD, structType)

  // 继续正常使用
  studentDF.registerTempTable("students")

  val teenagerDF = sqlContext.sql("select * from students where age <= 18")

  teenagerDF.rdd.collect().foreach{ row => println(row)}


}
