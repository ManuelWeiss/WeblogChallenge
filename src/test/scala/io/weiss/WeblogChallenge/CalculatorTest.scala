package io.weiss.WeblogChallenge

import org.apache.spark.sql.{Row, SparkSession}
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.{FlatSpec, Matchers}

@RunWith(classOf[JUnitRunner])
class CalculatorTest extends FlatSpec with Matchers {

  behavior of "Calculator"

  val sparkSession = SparkSession
    .builder()
    .appName("Test")
    .config("spark.master", "local")
    .getOrCreate()

  import sparkSession.implicits._

  it should "calculate average lengths and unique URLs for a single session" in {
    val lengths = Seq[Long](100, 200, 300)
    val urlCounts = Seq(5, 3, 1)
    val testData = Seq(
      "1.0.0.1" -> Seq(
        (lengths(0), urlCounts(0)),
        (lengths(1), urlCounts(1)),
        (lengths(2), urlCounts(2))
      )
    ).toDS()

    val result = Calculator.calcAverages(sparkSession, testData)

    result shouldBe (lengths.sum / 3, urlCounts.sum / 3)
  }

  it should "calculate average lengths and unique URLs per session" in {
    val lengths = Seq[Long](100, 200, 600, 100, 300, 200)
    val urlCounts = Seq(5, 3, 1, 1, 2, 6)
    val testData = Seq(
      "1.0.0.1" -> Seq(
        (lengths(0), urlCounts(0)),
        (lengths(1), urlCounts(1)),
        (lengths(2), urlCounts(2))
      ),
      "1.0.0.2" -> Seq(
        (lengths(3), urlCounts(3))
      ),
      "1.0.0.3" -> Seq(
        (lengths(4), urlCounts(4)),
        (lengths(5), urlCounts(5))
      )
    ).toDS()

    val result = Calculator.calcAverages(sparkSession, testData)

    result shouldBe (lengths.sum / 6, urlCounts.sum / 6)
  }

  it should "find users with longest sessions" in {
    val testData = Seq[(String, Seq[(Long, Int)])](
      "1.0.0.1" -> Seq(
        (1, 1),
        (2, 1),
        (3, 1)
      ),
      "1.0.0.2" -> Seq(
        (5, 1)
      ),
      "1.0.0.3" -> Seq(
        (4, 1),
        (1, 1)
      )
    ).toDS()

    val result = Calculator.calcTopUsers(sparkSession, testData)

    result.collect() shouldBe Array(
      Row("1.0.0.2", 5),
      Row("1.0.0.3", 4),
      Row("1.0.0.1", 3)
    )
  }
}
