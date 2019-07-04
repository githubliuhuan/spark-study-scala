package cn.spark.study.streaming

import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf}

object TransformBlacklist {

  val conf = new SparkConf()
    .setAppName("TransformBlacklist")
    .setMaster("local")

  val ssc = new StreamingContext(conf, Seconds(5))
  val blacklist = Array("tom", true)
  val blacklistRDD = ssc.sparkContext.parallelize(blacklist, 5)

  val adsClickLogDstream = ssc.socketTextStream("",9999)
  val userAdsClickLogDstream = adsClickLogDstream
    .map{ adsClickLog => (adsClickLog.split(" ")(1), adsClickLog)}

  val vaildAdsClickLogDstream = userAdsClickLogDstream.transform(userAdsClickLogRDD => {
    val joinedRDD = userAdsClickLogRDD.leftOuterJoin(blacklistRDD)
    val filteredRDD = joinedRDD
  })

}
