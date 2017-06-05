package io.weiss.WeblogChallenge

import io.weiss.WeblogChallenge.WeblogChallenge.sparkSession
import org.apache.spark.sql.{Dataset, SparkSession}

object Calculator {

  import sparkSession.implicits._

  def calcAverages(session: SparkSession,
                   sessionLengthsAndUrlCountsByIp: Dataset[(String, Seq[(Long, Int)])]): (Double, Double) = {
    val sessionCounter = sparkSession.sparkContext.longAccumulator
    val sessionLengthSum = sparkSession.sparkContext.longAccumulator
    val uniqueUrlsSum = sparkSession.sparkContext.longAccumulator
    sessionLengthsAndUrlCountsByIp.flatMap(_._2) foreach { x =>
      sessionCounter.add(1)
      sessionLengthSum.add(x._1)
      uniqueUrlsSum.add(x._2)
    }

    val averageSessionLength = sessionLengthSum.value / sessionCounter.value.toDouble
    val averageUniqueUrls = uniqueUrlsSum.value / sessionCounter.value.toDouble

    (averageSessionLength, averageUniqueUrls)
  }

  def calcTopUsers(session: SparkSession,
                   sessionLengthsAndUrlCountsByIp: Dataset[(String, Seq[(Long, Int)])]) = {
    val maxSessionLengthByIp = sessionLengthsAndUrlCountsByIp map { case (ip, lengthAndUrls) =>
      ip -> lengthAndUrls.foldLeft(0L)((a, b) => a.max(b._1))
    }
    maxSessionLengthByIp
      .withColumnRenamed("_1", "ip_address")
      .withColumnRenamed("_2", "max_session_length")
      .sort($"max_session_length".desc)
  }
}
