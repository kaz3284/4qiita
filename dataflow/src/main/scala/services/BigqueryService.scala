package services

import models.{Action, Segment, SegmentationResultTmp}
import com.spotify.scio._
import com.spotify.scio.bigquery._
import com.spotify.scio.values.SideInput
import models.bq.{SegmentationResult, UserHistory}
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write


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
  def apply(unitId: Int, input: String, output: String, sc: ScioContext, segmentsSi: SideInput[Iterable[Segment]]): Unit = {

    // bigqueryに入っている履歴データを処理に適したデータへ
    val userHistories = sc.bigQueryTable(input)
        .map {row =>
          val userId = row.getString("user_id")
          val actionHistory = row.getString("action_history").split('|').map(a => Action(a.toLong)).toList
          UserHistory(unitId, userId, actionHistory)
        }

    // マスタデータを副入力で取り込み,処理結果をbigqueryへ保存
    userHistories.withSideInputs(segmentsSi)
      .flatMap { (userHistory, si) =>
        val segments = si(segmentsSi)
        val actionIdCountMap = userHistory.actionHistory.groupBy(_.actionId).mapValues(_.size)
        findFilledSegmentIds(userHistory.userId, actionIdCountMap, segments)
          .map(r => SegmentationResult(r.unitId, r.userId, r.filledSegmentId))
      }.toSCollection
      .saveAsTypedBigQuery(output, Write.WriteDisposition.WRITE_TRUNCATE, Write.CreateDisposition.CREATE_IF_NEEDED)
  }

  // frequency条件を満たすSet(segmentId)を返す。
  private def findFilledSegmentIds(userId: String, actionIdCountMap: Map[Long, Int], segments: Iterable[Segment]): Set[SegmentationResultTmp] = {
    segments.filter { case s: Segment =>
      if (actionIdCountMap.get(s.actionId).isEmpty) false
      else s.isFill(actionIdCountMap.get(s.actionId))
    }.map{case s: Segment =>
      SegmentationResultTmp(s.unitId, userId, s.id, s.actionId, actionIdCountMap.get(s.actionId))
    }.toSet
  }
}
