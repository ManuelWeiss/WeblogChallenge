package io.weiss.WeblogChallenge

import java.sql.Timestamp

case class LogRow(time: Timestamp, ipAddress: String, url: String)

case class Visit(time: Timestamp, url: String)

case class SessionVisit(id: Int,
                        time: Timestamp,
                        url: String)
