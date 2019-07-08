package cn.spark.caifu


object ReadTxt {

  def main(args: Array[String]): Unit = {
    import scala.io.Source
    val file=Source.fromFile("E:\\test.txt")
    // getLines返回的是一个迭代器
    var sqlUnion = ""
    for(line <- file.getLines)
    {
      var sql = sqlUnion + s"select '${line}' as table_name,count(*),from_unixtime(unix_timestamp(),'yyyy-MM-dd HH:mm:ss') as check_time from ${line}"

      println(sql)
    }
    file.close

  }
}
