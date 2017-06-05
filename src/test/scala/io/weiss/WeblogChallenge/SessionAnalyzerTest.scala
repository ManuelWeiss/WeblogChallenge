package io.weiss.WeblogChallenge

import java.sql.Timestamp

import org.junit.runner.RunWith
import org.scalatest.{FlatSpec, Matchers}
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class SessionAnalyzerTest extends FlatSpec with Matchers {

  behavior of "SessionAnalyzer"

  it should "create a session from a single visit" in {
    val visit = Visit(new Timestamp(123), "example.com")
    val sessions = SessionAnalyzer.makeSessions(Vector(visit))

    sessions shouldBe Vector(SessionVisit(1, visit.time, visit.url))
  }

  it should "create a session from several visits, with correct order" in {
    val visit1 = Visit(new Timestamp(123), "a.example.com")
    val visit2 = Visit(new Timestamp(234), "b.example.com")
    val visit3 = Visit(new Timestamp(345), "c.example.com")
    val visit4 = Visit(new Timestamp(456), "d.example.com")

    val visits = Vector(visit3, visit1, visit4, visit2)

    val sessions = SessionAnalyzer.makeSessions(visits)

    sessions shouldBe Vector(
      SessionVisit(1, visit1.time, visit1.url),
      SessionVisit(1, visit2.time, visit2.url),
      SessionVisit(1, visit3.time, visit3.url),
      SessionVisit(1, visit4.time, visit4.url)
    )
  }

  it should "create separate sessions for visits more than X minutes apart" in {
    val sessionTimeout = 3
    val visit1 = Visit(new Timestamp(123), "a.example.com")
    val visit2 = Visit(new Timestamp(234), "b.example.com")
    val visit3 = Visit(new Timestamp(minutesToMillis(sessionTimeout + 1)), "c.example.com")
    val visit4 = Visit(new Timestamp(minutesToMillis(2 * sessionTimeout + 2)), "d.example.com")

    val visits = Vector(visit3, visit1, visit4, visit2)

    val sessions = SessionAnalyzer.makeSessions(visits, sessionTimeout)

    sessions shouldBe Vector(
      SessionVisit(1, visit1.time, visit1.url),
      SessionVisit(1, visit2.time, visit2.url),
      SessionVisit(2, visit3.time, visit3.url),
      SessionVisit(3, visit4.time, visit4.url)
    )
  }

  it should "calculate the length of sessions" in {
    val sessions = Vector(
      SessionVisit(1, new Timestamp(1),   "a.example.com"),
      SessionVisit(1, new Timestamp(3),   "a.example.com"),
      SessionVisit(2, new Timestamp(10),  "a.example.com"),
      SessionVisit(2, new Timestamp(12),  "a.example.com"),
      SessionVisit(2, new Timestamp(15),  "a.example.com"),
      SessionVisit(3, new Timestamp(0),   "a.example.com"),
      SessionVisit(3, new Timestamp(10),  "a.example.com"),
      SessionVisit(3, new Timestamp(100), "a.example.com")
    )

    val sessionLengths = SessionAnalyzer.calcSessionLengthsInMillis(sessions)
    sessionLengths should contain theSameElementsAs Seq(
      (3 - 1),
      (15 - 10),
      100
    )
  }

  it should "calculate the number of unique URLs per sessions" in {
    val sessions = Vector(
      SessionVisit(1, new Timestamp(1),   "a.example.com"),
      SessionVisit(1, new Timestamp(3),   "a.example.com"),
      SessionVisit(2, new Timestamp(10),  "a.example.com"),
      SessionVisit(2, new Timestamp(12),  "b.example.com"),
      SessionVisit(2, new Timestamp(15),  "c.example.com"),
      SessionVisit(3, new Timestamp(0),   "a.example.com"),
      SessionVisit(3, new Timestamp(10),  "b.example.com"),
      SessionVisit(3, new Timestamp(100), "b.example.com")
    )

    val sessionLengths = SessionAnalyzer.uniqueUrlsPerSession(sessions)
    sessionLengths should contain theSameElementsAs Seq(
      1,
      3,
      2
    )
  }

  private def minutesToMillis(mins: Int) = mins * 60 * 1000L
}
