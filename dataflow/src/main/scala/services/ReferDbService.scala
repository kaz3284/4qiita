package services

import models.Segment
import scalikejdbc._


object ReferDbService {

  implicit val session = AutoSession
  val sf = Segment.syntax

  def findSegments(jdbcUri: String, dbUser: String): List[Segment] = {
    Class.forName("com.mysql.jdbc.Driver")
    scalikejdbc.ConnectionPool.singleton(jdbcUri, dbUser, "")

    withSQL {
      select.from(Segment as sf)
    }.map(Segment(sf.resultName))
      .list
      .apply()
  }
}