package org.apache.spark.streaming

import org.apache.spark.{Logging, SparkConf}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.storage.StorageLevel

/**
 *  `$ nc -lk 9999`で入力データを設定する。
 */
object NetworkWordCountSQL extends Logging {
  def main(args: Array[String]) {
    val sparkConf = new SparkConf().setAppName("NetworkWordCountSQL")
    val ssc = new StreamingContext(sparkConf, Seconds(10))

    // Create a socket stream on target ip:port and count the
    val windowData = ssc.socketTextStream("localhost", 9999, StorageLevel.MEMORY_AND_DISK_SER)

    windowData.foreachRDD{rdd =>
      // Get the singleton instance of SQLContext
      val sqlContext = SQLContext.getOrCreate(rdd.sparkContext)
      import sqlContext.implicits._

      // Convert RDD[String] to DataFrame
      val wordsDataFrame = rdd.toDF("word")
    }


    windowData.foreachRDD{ rdd =>
      // この部分に書いたコードはdriverで実行される。
      rdd.foreachPartition{ partedRdd =>
        // この部分に書いたコードはexecutorで実行される。

      }
    }

//    val rows = lines.flatMap(_.split("¥n"))
//    rows.foreachRDD { jsonRdd =>
//      val rddSc = SQLContext.getOrCreate(jsonRdd.sparkContext)
//      if (jsonRdd.count() > 0) {
//        val tpLogs = sqlContext.read.json(jsonRdd)
//        tpLogs.registerTempTable("tmp_new_log")
//        val sumDF = rddSc.sql(
//          """
//                                    SELECT client_id, campaign_id, ad_group_id, media_id, channel_id, SUM(imp) AS imps, SUM(vimp) AS vimps, SUM(click) AS clicks, SUM(cost) AS costs
//                                    FROM (
//                                    SELECT client_id, campaign_id, ad_group_id, media_id, channel_id, imp, vimp, click, cost
//                                      FROM ad_effect
//                                    UNION ALL
//                                    SELECT cl AS client_id, ca AS campaign_id, ag AS ad_group_id, me AS media_id, ch AS channel_id, COALESCE(count(*),0) AS imp, 0 AS vimp, 0 AS click, 0 AS cost
//                                      FROM tmp_new_log
//                                      WHERE tp = 'imp' AND cl IS NOT NULL AND ca IS NOT NULL AND ag IS NOT NULL AND ch IS NOT NULL AND me IS NOT NULL
//                                      GROUP BY cl, ca, ag, ch, me
//                                    UNION ALL
//                                    SELECT cl AS client_id, ca AS campaign_id, ag AS ad_group_id, me AS media_id, ch AS channel_id, 0 AS imp, COALESCE(count(*),0) AS vimp, 0 AS click, 0 AS cost
//                                      FROM tmp_new_log
//                                      WHERE tp = 'vimp' AND cl IS NOT NULL AND ca IS NOT NULL AND ag IS NOT NULL AND ch IS NOT NULL AND me IS NOT NULL
//                                      GROUP BY cl, ca, ag, ch, me
//                                    UNION ALL
//                                    SELECT cl AS client_id, ca AS campaign_id, ag AS ad_group_id, me AS media_id, ch AS channel_id, 0 AS imp, 0 AS vimp, SUM(cs) AS cost, count(*) AS click
//                                      FROM
//                                      (SELECT DISTINCT cl, ca, ag, me, ch, si, cs
//                                        FROM tmp_new_log
//                                        WHERE tp = 'click' AND cl IS NOT NULL AND ca IS NOT NULL AND ag IS NOT NULL AND ch IS NOT NULL AND me IS NOT NULL) sub
//                                      GROUP BY cl, ca, ag, me, ch
//                                    ) sub2 GROUP BY client_id, campaign_id, ad_group_id, media_id, channel_id
//          """)
//        sumDF.registerTempTable("sumDF")
//        println("sumDF")
//        sumDF.show()
//      }
//    }


    //
    ssc.start()
    ssc.awaitTermination()
  }
}