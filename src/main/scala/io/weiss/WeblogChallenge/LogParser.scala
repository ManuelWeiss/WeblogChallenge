package io.weiss.WeblogChallenge

import java.time.ZonedDateTime
import java.sql.Timestamp

import io.weiss.WeblogChallenge.WeblogChallenge.sparkSession
import org.apache.spark.sql.{Dataset, Row, SparkSession}
import org.apache.spark.sql.types._

object LogParser {

  import sparkSession.implicits._

  // timestamp elb client:port backend:port request_processing_time backend_processing_time response_processing_time
  // elb_status_code backend_status_code received_bytes sent_bytes "request" "user_agent" ssl_cipher ssl_protocol
  val schema = StructType(List(
    StructField("timestamp", StringType),
    StructField("elb", StringType),
    StructField("ip", StringType),
    StructField("bp", StringType),
    StructField("req_t", StringType),
    StructField("bpt", StringType),
    StructField("resp_t", StringType),
    StructField("elb_status", IntegerType),
    StructField("be_status", IntegerType),
    StructField("received", IntegerType),
    StructField("sent", IntegerType),
    StructField("request", StringType),
    StructField("user_agent", StringType),
    StructField("cipher", StringType),
    StructField("protocol", StringType)
  ))

  def parseLog(sparkSession: SparkSession, path: String): Dataset[LogRow] = {
    sparkSession.read
      .option("header", false)
      .option("sep", " ")
      .schema(schema)
      .csv(path)
      .map(parseLine)
  }

  private def parseLine(row: Row) = {
    val time = new Timestamp(ZonedDateTime.parse(row.getString(0)).toInstant().toEpochMilli)
    val ip = row.getString(2).split(":")(0)
    val url = row.getString(11).split(" ")(1)
    LogRow(time, ip, url)
  }
}
