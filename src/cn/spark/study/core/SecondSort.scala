package cn.spark.study.core
import org.apache.spark.{SparkConf, SparkContext}

object SecondSort {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setAppName("SecondSort")
      .setMaster("local")
    val sc = new SparkContext(conf)

    val lines = sc.textFile("", 1)
    val pairs = lines.map{ line => {
      new SecondSortKey(line.split(" ")(0).toInt, line.split(" ")(1).toInt),
      line}}

    val sortedPairs = pair.


  }
}
