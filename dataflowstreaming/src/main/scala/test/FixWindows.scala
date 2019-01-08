package test

import java.util.TimeZone

import com.spotify.scio._
import com.spotify.scio.bigquery._
import com.spotify.scio.streaming._
import com.spotify.scio.values.{SCollection, WindowOptions}
import org.apache.beam.examples.common.{ExampleOptions, ExampleUtils}
import org.apache.beam.sdk.options.StreamingOptions
import org.apache.beam.sdk.transforms.windowing._
import org.joda.time.format.DateTimeFormat
import org.joda.time.{DateTimeZone, Duration, Instant}
import test.UserScore.GameActionInfo

import scala.util.Try

object FixWindows {

  // The schemas for the BigQuery tables to write output to are defined as annotated case classes
  @BigQueryType.toTable
  case class TeamScoreSums(team: String, total_score: Int,
                           window_start: String, processing_time: String, timing: String)
  @BigQueryType.toTable
  case class UserScoreSums(user: String, total_score: Int, processing_time: String)

  // scalastyle:off method.length
  def main(cmdlineArgs: Array[String]): Unit = {
    // Create `ScioContext` and `Args`
    val (opts, args) = ScioContext.parseArguments[ExampleOptions](cmdlineArgs)
    val sc = ScioContext(opts)
    sc.optionsAs[StreamingOptions].setStreaming(true)
    val exampleUtils = new ExampleUtils(sc.options)

    // Date formatter for full timestamp
    def fmt = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss.SSS")
      .withZone(DateTimeZone.forTimeZone(TimeZone.getTimeZone("JST")))
    // Duration in minutes over which to calculate team scores, defaults to 1 hour
    val teamWindowDuration = Duration.standardMinutes(args.int("teamWindowDuration", 60))
    // Data that comes in from our streaming pipeline after this duration isn't considered in our
    // processing. Measured in minutes, defaults to 2 hours
    val allowedLateness = Duration.standardMinutes(args.int("allowedLateness", 120))

    // Read in streaming data from PubSub and parse each row as `GameActionInfo` events
    val gameEvents = sc.pubsubTopic(args("topic"), timestampAttribute = "timestamp_ms")
      .flatMap(UserScore.parseEvent)

    ///////////
    // TeamScore
    calculateTeamScores(gameEvents, teamWindowDuration, allowedLateness)
      // Add windowing information to team score results by converting to `WindowedSCollection`
      .toWindowed
      .map { wv =>
        // Convert from score tuple to TeamScoreSums object with both tuple and windowing info
        val start = fmt.print(wv.window.asInstanceOf[IntervalWindow].start())
        val now = fmt.print(Instant.now())
        val timing = wv.pane.getTiming.toString
        wv.copy(value = TeamScoreSums(wv.value._1, wv.value._2, start, now, timing))
      }
      // Done with windowing information, convert back to regular `SCollection`
      .toSCollection
      // Save to the BigQuery table defined by "output" in the arguments passed in + "_team" suffix
      .saveAsTypedBigQuery(args("output") + "_team")

    ////////////
    // UserScore
    caluculateUserScores(gameEvents, teamWindowDuration, allowedLateness)
      // Map summed results from tuples into `UserScoreSums` case class, so we can save to BQ
      .map(kv => UserScoreSums(kv._1, kv._2, fmt.print(Instant.now())))
      // Save to the BigQuery table defined by "output" in the arguments passed in + "_user" suffix
      .saveAsTypedBigQuery(args("output") + "_user")

    // Close context and execute the pipeline
    val result = sc.close()
    // Wait to finish processing before exiting when streaming pipeline is canceled during shutdown
    exampleUtils.waitToFinish(result.internal)
  }

  /**
    *
    * @param infos
    * @return
    */
  def calculate1(infos: SCollection[GameActionInfo]): SCollection[(String, Int)] =
    infos.withFixedWindows(Duration.standardMinutes(2))
      .map(i => (i.team, i.score))
      .sumByKey

  /**
    *
    * @param infos
    * @return
    */
  def calculate2(infos: SCollection[GameActionInfo]): SCollection[(String, Int)] =
    infos.withFixedWindows(Duration.standardMinutes(2),
      options = WindowOptions(
        trigger = AfterWatermark.pastEndOfWindow()
          .withEarlyFirings(AfterProcessingTime.pastFirstElementInPane()
            .plusDelayOf(Duration.standardMinutes(1)))
          .withLateFirings(AfterProcessingTime.pastFirstElementInPane()
            .plusDelayOf(Duration.standardMinutes(5))),
        accumulationMode = ACCUMULATING_FIRED_PANES,
        allowedLateness = Duration.standardMinutes(600)))
      .map(i => (i.team, i.score))
      .sumByKey

  /**
    *
    * @param infos
    * @param teamWindowDuration
    * @param allowedLateness
    * @return
    */
  def calculateTeamScores(infos: SCollection[GameActionInfo],
                          teamWindowDuration: Duration,
                          allowedLateness: Duration): SCollection[(String, Int)] =
    infos.withFixedWindows(
      teamWindowDuration,
      options = WindowOptions(
        trigger = AfterWatermark.pastEndOfWindow()
          .withEarlyFirings(AfterProcessingTime.pastFirstElementInPane()
            .plusDelayOf(Duration.standardMinutes(5)))
          .withLateFirings(AfterProcessingTime.pastFirstElementInPane()
            .plusDelayOf(Duration.standardMinutes(10))),
        accumulationMode = ACCUMULATING_FIRED_PANES,
        allowedLateness = allowedLateness))
      // Change each event into a tuple of: team user was on, and that user's score
      .map(i => (i.team, i.score))
      // Sum the scores across the defined window, using "team" as the key to sum by
      .sumByKey

  /**
    *
    * @param infos
    * @param userWindowDuration
    * @param allowedLateness
    * @return
    */
  def caluculateUserScores(infos: SCollection[GameActionInfo],
                           userWindowDuration: Duration,
                           allowedLateness: Duration): SCollection[(String, Int)] =
    infos.withGlobalWindow(WindowOptions(
      trigger = Repeatedly.forever(AfterProcessingTime.pastFirstElementInPane()
        .plusDelayOf(userWindowDuration)),
      accumulationMode = ACCUMULATING_FIRED_PANES,
      allowedLateness = allowedLateness)
      )
      // Change each event into a tuple of: user, and that user's score
      .map(i => (i.user, i.score))
      // Sum the scores by user
      .sumByKey

}


object UserScore {

  // Case class containing all the fields within an event, for internal model
  case class GameActionInfo(user: String, team: String, score: Int, timestamp: Long)

  // The schema for the BigQuery table to write output to is defined as an annotated case class
  @BigQueryType.toTable
  case class UserScoreSums(user: String, total_score: Int)

  // Helper function for parsing data. Reads in a CSV line and converts to `GameActionInfo` instance
  def parseEvent(line: String): Option[GameActionInfo] = Try {
    val t = line.split(",")
    GameActionInfo(t(0).trim, t(1).trim, t(2).toInt, t(3).toLong)
  }.toOption
}
