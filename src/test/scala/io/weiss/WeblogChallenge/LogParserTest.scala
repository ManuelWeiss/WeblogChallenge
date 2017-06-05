package io.weiss.WeblogChallenge

import java.nio.file.Paths

import org.apache.spark.sql.{Row, SparkSession}
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.{FlatSpec, Matchers}

@RunWith(classOf[JUnitRunner])
class LogParserTest extends FlatSpec with Matchers {

  behavior of "LogParser"

  val sparkSession = SparkSession
    .builder()
    .appName("Test")
    .config("spark.master", "local")
    .getOrCreate()

  it should "parse an ELB log file" in {
    val logFile = "/test.log"
    val path = Paths.get(getClass.getResource(logFile).toURI).toString
    val data = LogParser.parseLog(sparkSession, path)

    val result = data.collect()

    result.map(_.toString) shouldBe Array(
      "LogRow(2015-07-22 10:00:28.019,123.242.248.130,https://paytm.com:443/shop/authresponse?code=f2405b05-e2ee-4b0d-8f6a-9fed0fcfe2e0&state=null)",
      "LogRow(2015-07-22 10:00:27.894,203.91.211.44,https://paytm.com:443/shop/wallet/txnhistory?page_size=10&page_number=0&channel=web&version=2)",
      "LogRow(2015-07-22 10:00:27.885,1.39.32.179,https://paytm.com:443/shop/wallet/txnhistory?page_size=10&page_number=0&channel=web&version=2)",
      "LogRow(2015-07-22 10:00:28.048,180.179.213.94,https://paytm.com:443/shop/p/micromax-yu-yureka-moonstone-grey-MOBMICROMAX-YU-DUMM141CD60AF7C_34315)"
    )
  }
}
