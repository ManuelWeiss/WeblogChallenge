package io.weiss.WeblogChallenge

import java.sql.Timestamp

object SessionAnalyzer {

  implicit val timestampOrdering = new Ordering[Timestamp] {
    def compare(t1: Timestamp, t2: Timestamp) = t1.compareTo(t2)
  }

  def makeSessions(visits: Seq[Visit], timeoutMins: Int = 15): Vector[SessionVisit] = {
    assert(visits.nonEmpty, "must contain at least one entry")

    val sortedVisits = visits.sortBy(_.time)
    val firstVisit = sortedVisits.head
    val sessionInit = SessionVisit(1, firstVisit.time, firstVisit.url)

    sortedVisits
      .foldLeft((sessionInit, Vector.empty[SessionVisit])) {
        case ((currentSession, acc), visit) =>
          if (isTimedOut(visit.time, currentSession.time, timeoutMins)) {
            val newSession = SessionVisit(currentSession.id + 1, visit.time, visit.url)
            newSession -> (acc :+ newSession)
          }
          else {
            val continuedSession = currentSession.copy(time = visit.time, url = visit.url)
            continuedSession -> (acc :+ continuedSession)
          }
      }._2
  }

  private def isTimedOut(t1: Timestamp, t2: Timestamp, timeoutMins: Int) =
    t1.toLocalDateTime.minusMinutes(timeoutMins).isAfter(t2.toLocalDateTime)

  def calcSessionLengthsInMillis(sv: Seq[SessionVisit]): Seq[Long] =
    sv.groupBy(_.id)
      .map { case (sessionId, visitsInOneSession) =>
        visitsInOneSession.last.time.getTime - visitsInOneSession.head.time.getTime
      }.toSeq

  def uniqueUrlsPerSession(sv: Seq[SessionVisit]): Seq[Int] =
    sv.groupBy(_.id)
      .map { case (sessionId, visitsInOneSession) =>
        visitsInOneSession.map(_.url).toSet.size
      }.toSeq
}
