package io.weiss.WeblogChallenge

import java.nio.file.Paths

object WeblogChallenge {

  import org.apache.spark.sql.SparkSession

  val sparkSession = SparkSession
      .builder()
      .appName("Weblog Challenge")
      .config("spark.master", "local")
      .getOrCreate()

  import sparkSession.implicits._

  def main(args: Array[String]): Unit = {
    if (args.size < 1) {
      Console.err.println("\n ** ERROR: please provide log file as first argument\n")
      return
    }

    val path = Paths.get(args(0)).toString
    val data = LogParser.parseLog(sparkSession, path)

    val sessions = data
      .groupByKey(_.ipAddress)
      .mapValues(lr => Seq(Visit(lr.time, lr.url)))
      .reduceGroups(_ ++ _)
      .map { case (ip, visits) => ip -> SessionAnalyzer.makeSessions(visits) }

    val sessionLengthsAndUrlsByIp = sessions map { case (ip, sessionVisits) =>
      ip -> SessionAnalyzer.calcSessionLengthsInMillis(sessionVisits).zip(SessionAnalyzer.uniqueUrlsPerSession(sessionVisits))
    }

    val (averageSessionLength, averageUniqueUrls) = Calculator.calcAverages(sparkSession, sessionLengthsAndUrlsByIp)

    val maxUsers = 10
    val sessionLengthToIp = Calculator.calcTopUsers(sparkSession, sessionLengthsAndUrlsByIp)
    val topUsers = sessionLengthToIp.head(maxUsers)

    println(s"average session length (in seconds): ${averageSessionLength / 1000}")
    println(s"average number of unique URLs/session: $averageUniqueUrls")
    println(s"Top $maxUsers most engaged users (longest sessions, in secs):")
    println(topUsers.map(r => "%15s %10.3f".format(r.getString(0), r.getLong(1)/1000d)).mkString("\n"))
  }

}
