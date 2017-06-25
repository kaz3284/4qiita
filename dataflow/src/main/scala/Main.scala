package main

import com.spotify.scio.ContextAndArgs
import services._

object SegmentationMain {

  def main(cmdlineArgs: Array[String]): Unit = {
    val (sc, args) = ContextAndArgs(cmdlineArgs)

    val (unitId, jdbcUri, dbUser, input, output) = (args("unitId").toLong, args("jdbcUri").toString, args("dbUser").toString, args("input"), args("output"))

    // DB(mysql)からマスタ情報取得
    val segmentsSi = sc.parallelize(ReferDbService.findSegments(jdbcUri, dbUser)).asIterableSideInput

    // ユーザを行動履歴を元にsegmentation(unit_idは1へ指定)
    UserSegmentation(1,input, output, sc, segmentsSi)

    sc.close()
  }

}