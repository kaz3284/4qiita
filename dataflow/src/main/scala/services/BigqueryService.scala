package services

import com.google.api.services.bigquery.model.{TableFieldSchema, TableSchema}
import models.{Segment, UserInfo}
import com.spotify.scio._
import com.spotify.scio.bigquery._
import com.spotify.scio.values.{SCollection, SideInput}
import models.bq.{Action, UserHistory}
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO

import scala.collection.JavaConverters._

object UserSegmentation {

  /**
    *
    * @param unitId
    * @param input
    * @param output
    * @param sc
    * @param segmentsSi
    * @return
    */
  def apply(unitId: Long, input: String, output: String, sc: ScioContext, segmentsSi: SideInput[Iterable[Segment]]): SCollection[UserInfo] = {

    // bqに入っているデータを取り出して
    val userInfos = sc.bigQuerySelect("SELECT * FROM [sample:user_history_sample]")
        .withSideInputs(segmentsSi)
        .map {(row, ctx) =>
          val userId = row.getString("user_id")
          val actionHistory = row.getString("user_history").split('|').map(a => Action(a.toLong)).toList
          UserHistory(unitId, userId, actionHistory)
        }.map { (d, ctx) =>
          val segments = ctx(segmentsSi)
          val actionIdCountMap = d.actionHistory.groupBy(_.actionId).mapValues(_.foldLeft(0)((z, n) => z + 1))
          val filledSegmentIds = findFilledSegmentIds(segments, actionIdCountMap)
          UserInfo(d.unitId, d.userId, filledSegmentIds, actionIdCountMap)
      }.toSCollection

    return userInfos
  }

  // frequency条件を満たすSet(segmentId)を返す。
  private def findFilledSegmentIds(segments: Iterable[Segment], actionIdCountMap: Map[Long, Int]): Set[Segment] = {
    segments.filter { case s =>
      val actionIdCount = actionIdCountMap(s.actionId)
      s.isFill(actionIdCount) != Some(false)
    }.toSet
  }
}

object SaveSegmentation {

  /**
    *
    * @param userInfos
    * @param output
    */
  def apply(userInfos: SCollection[UserInfo],output: String): Unit ={
    userInfos.saveAsTextFile(output)
  }
}