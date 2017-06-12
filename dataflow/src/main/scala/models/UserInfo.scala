package models

/**
  *
  * @param unitId
  * @param userId
  * @param filledSegmentIds
  * @param actionCountMap
  */
case class UserInfo(unitId: Long, userId: String, filledSegmentIds: Set[Segment], actionCountMap: Map[Long, Int])

