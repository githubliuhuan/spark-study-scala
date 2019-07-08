package cn.spark.caifu

import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

object YiXinCheckCount {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setAppName("YiXinCheckCount")
    val sc = new SparkContext(conf)
    val hivecontext = new HiveContext(sc)
    val sqlUnionAll = readTxt("/opt/program/work/dev/liuhuan/spark-study-scala/spark-sql/monitor_table.txt")
    val checkTableDF = hivecontext.sql("sqlUnionAll")
    checkTableDF.write.mode(SaveMode.Append).format("hive").saveAsTable("dm.table_count")
    println(checkTableDF.collect().foreach(println))

  }

  def readTxt(filePath: String): String = {
    import scala.io.Source
    val file=Source.fromFile(filePath)
    // getLines返回的是一个迭代器
    var sqlUnion = ""
    for(line <- file.getLines)
    {
      var sql = s"select '${line}' as table_name,count(*),from_unixtime(unix_timestamp(),'yyyy-MM-dd HH:mm:ss') as check_time from ${line} union all"
      sqlUnion += sql
    }
    val sqlUnionAll = sqlUnion.substring(0,sqlUnion.length - 10)
    file.close
    sqlUnionAll
  }
}
