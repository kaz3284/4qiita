name := "dataflowstreaming"

version := "0.1"

scalaVersion := "2.12.8"

val beamVersion = "2.6.0"

libraryDependencies ++= Seq(
  "com.spotify" %% "scio-core" % "0.6.0",
  "org.apache.beam" % "beam-sdks-java-core" % beamVersion,
  "org.apache.beam" % "beam-examples-java" % beamVersion,
  "org.apache.beam" % "beam-runners-direct-java" % beamVersion,
  "org.apache.beam" % "beam-runners-google-cloud-dataflow-java" % beamVersion
)

addCompilerPlugin("org.scalamacros" % "paradise" % "2.1.0" cross CrossVersion.full)