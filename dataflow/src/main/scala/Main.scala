package main

import com.spotify.scio.ContextAndArgs
import services._

object Main {

  def main(cmdlineArgs: Array[String]): Unit = {
    val (sc, args) = ContextAndArgs(cmdlineArgs)

    val (unitId, input, output) = (args("unitId").toLong, args("input"), args("output"))

    // DB(mysql)からマスタ情報取得
    val segmentsSi = sc.parallelize(FindSegments()).asIterableSideInput

    // ユーザを行動履歴を元にsegmentation(userInfo)
    val userInfos = UserSegmentation(1,input, output, sc, segmentsSi)

    // segmentation結果をbqへ永続化
    SaveSegmentation(userInfos,output)

    sc.close()
  }

}