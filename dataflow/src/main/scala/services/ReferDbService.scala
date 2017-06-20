package services

import models.Segment
import scalikejdbc._


object FindSegments {

  Class.forName("com.mysql.jdbc.Driver")
  scalikejdbc.ConnectionPool.singleton("jdbc:mysql://stg-gidb1:3306/dataflow_test", "rssad", "")
  implicit val session = AutoSession
  val sf = Segment.syntax

  def apply(): List[Segment] = {
    withSQL {
      select.from(Segment as sf)
    }.map(Segment(sf.resultName))
      .list
      .apply()
  }
}