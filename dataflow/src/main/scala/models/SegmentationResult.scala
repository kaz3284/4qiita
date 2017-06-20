package models

import com.spotify.scio.bigquery.types.BigQueryType

/**
  *
  * @param unitId
  * @param userId
  * @param filledSegmentId
  * @param actionId
  * @param actionCount
  */
case class SegmentationResult(unitId: Long, userId: String, filledSegmentId: Long, actionId: Long, actionCount: Option[Int])

