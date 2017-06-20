package services

import models.{Action, Segment, SegmentationResult}
import com.spotify.scio._
import com.spotify.scio.bigquery._
import com.spotify.scio.values.{SCollection, SideInput}
import models.bq.UserHistory

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
  def apply(unitId: Long, input: String, output: String, sc: ScioContext, segmentsSi: SideInput[Iterable[Segment]]): SCollection[Set[SegmentationResult]] = {

    // bqに入っているデータを取り出して
    val userHistories = sc.bigQueryTable(input)
        .map {row =>
          val userId = row.getString("user_id")
          val actionHistory = row.getString("action_history").split('|').map(a => Action(a.toLong)).toList
          UserHistory(unitId, userId, actionHistory)
        }

    val segmentationResults = userHistories.withSideInputs(segmentsSi)
      .map { (userHistory, si) =>
          val segments = si(segmentsSi)
          val actionIdCountMap = userHistory.actionHistory.groupBy(_.actionId).mapValues(_.size)
          findFilledSegmentIds(userHistory.userId, actionIdCountMap, segments)
      }.toSCollection

    return segmentationResults
  }

  object SaveSegmentation {

    /**
      *
      * @param userInfos
      * @param output
      */
    def apply(userInfos: SCollection[Set[SegmentationResult]], output: String): Unit ={
      userInfos.saveAsTextFile(output)
    }
  }

  // frequency条件を満たすSet(segmentId)を返す。
  private def findFilledSegmentIds(userId: String, actionIdCountMap: Map[Long, Int], segments: Iterable[Segment]): Set[SegmentationResult] = {
    segments.filter { case s: Segment =>
      if (actionIdCountMap.get(s.actionId).isEmpty) false
      else s.isFill(actionIdCountMap.get(s.actionId))
    }.map{case s: Segment =>
      SegmentationResult(s.unitId, userId, s.id, s.actionId, actionIdCountMap.get(s.actionId))
    }.toSet
  }
}
