package cn.spark.study.sql

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

object JDBCDataSource {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    val studentInfosDF = sqlContext.read.format("jdbc").options(
      Map("url" -> "jdbc:mysql://spark1:3306/testdb",
        "dbtable" -> "student_infos")).load()

    val studentScoresDF = sqlContext.read.format("jdbc").options(
      Map("url" -> "jdbc:mysql://spark1:3306/testdb",
        "dbtable" -> "student_scores")).load()


  }
}
