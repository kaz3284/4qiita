package models.bq

import com.spotify.scio.bigquery.types.BigQueryType


/**
  *
  * @param unitId
  * @param userId
  * @param segmentId
  */
@BigQueryType.toTable
case class SegmentationResult(unitId: Int, userId: String, segmentId: Long)