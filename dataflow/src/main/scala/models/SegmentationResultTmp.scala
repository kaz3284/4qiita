package models

/**
  *
  * @param unitId
  * @param userId
  * @param filledSegmentId
  * @param actionId
  * @param actionCount
  */
case class SegmentationResultTmp(unitId: Int, userId: String, filledSegmentId: Long, actionId: Long, actionCount: Option[Int])

