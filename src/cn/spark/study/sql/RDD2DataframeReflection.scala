package cn.spark.study.sql

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

object RDD2DataframeReflection extends App {
  val conf = new SparkConf()
    .setMaster("local")
    .setAppName("RDD2DataframeReflection")
  val sc = new SparkContext(conf)
  val sqlContext = new SQLContext(sc)

  // 在scala中使用反射方式，进行RDD到Dataframe的转换，需要手动导入一个隐式转换
  import sqlContext.implicits._

  case class Student(id: Int, name: String, age: Int)

  // 这里其实就是一个普通的，元素为case class的RDD
  // 直接对它使用toDF()方法，即可转换为DataFrame
  val studentDF = sc.textFile("", 1)
    .map( lines => lines.split(","))
    .map( arr => Student(arr(0).trim().toInt, arr(1), arr(2).trim().toInt))
    .toDF()

  studentDF.registerTempTable("students")

  val teenagerDF = sqlContext.sql("select * from students where age <= 18")

  val teenagerRDD = teenagerDF.rdd

  teenagerRDD.map( row => Student(row(0).toString().toInt, row(1).toString(), row(2).toString().toInt))
    .collect()
    .foreach{ stu => println(stu.id + ":" + stu.name + ":" + stu.age)}

  // 在scala中，对row的使用，比java中的row的使用，更加丰富
  // 在scala中，可以用row的getAs()方法，获取指定列名的列
  teenagerRDD.map( row => Student(row.getAs[Int]("id"), row.getAs[String]("name"), row.getAs[Int]("age")))
    .collect()
    .foreach{ stu => println(stu.id + ":" + stu.name + ":" + stu.age)}
  // 还可以用row的getValuesMap()方法，获取指定几列的值，返回的是个map
  val StudentRDD = teenagerRDD.map{
    row => {
      val map = row.getValuesMap[Any](Array("id", "name", "age"))
      Student(map("id").toString().toInt, map("name").toString(), map("age").toString().toInt)
    }
  }
  StudentRDD.collect().foreach{ stu => println(stu.id + ":" + stu.name + ":" + stu.age)}

}

