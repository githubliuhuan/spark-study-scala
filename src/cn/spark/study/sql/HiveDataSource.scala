package cn.spark.study.sql

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.hive.HiveContext

object HiveDataSource {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
    .setAppName("HiveDataSource")
    .setMaster("local")

  val sc = new SparkContext(conf)

  val hivecontext = new HiveContext(sc)

  hivecontext.sql("drop table if exists student_infos")
  hivecontext.sql("create table if not exists studnet_infos (name string, age int)")
  hivecontext.sql("load data" +
    "local inpath '/usr/local/spark-study/resources/student_infos.txt'" +
    "into table student_infos")

  hivecontext.sql("drop table if exists student_scores")
  hivecontext.sql("create table if not exists student_scores (name string, score int)")
  hivecontext.sql("load data" +
    "local inpath '/usr/local/spark-study/resources/student_scores.txt'" +
    "into table student_scores")

  val goodStudentDF = hivecontext.sql("select si.name,si.age,ss.score" +
    "from student_infos si" +
    "join student_scores ss on si.name = ss.name" +
    "where ss.score >= 80")

  hivecontext.sql("drop table if exists good_student_infos")
  goodStudentDF.write.saveAsTable("good_student_infos")

  val goodStudentRows = hivecontext.table("good_student_infos").collect()
  for (good_StudentRow <- goodStudentRows) {
    println(good_StudentRow)
  }
}
}
