package org.apache.spark.streaming

import org.apache.spark.sql.SQLContext
import org.apache.spark.{Logging, SparkConf}
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
    
    windowData.foreachRDD{jsonRdd =>
      if (jsonRdd.count() > 0) {
        val sqlContext = SQLContext.getOrCreate(jsonRdd.sparkContext)
        val rowDf = sqlContext.read.json(jsonRdd)
        println("Summary by DataFrame")
        rowDf.groupBy("a_id","b_id").sum("count").show()

        println("Summary by SQL")
        rowDf.registerTempTable("json_data")
        val sumAdf = sqlContext.sql("SELECT a_id, b_id, sum(count) AS SUM FROM json_data GROUP BY a_id, b_id")
        sumAdf.show()
      }
    }

    ssc.start()
    ssc.awaitTermination()
  }
}
