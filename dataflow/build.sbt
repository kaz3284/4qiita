name := "dataflow"

version := "0.0.1"

scalaVersion := "2.11.11"

val beamVersion = "0.6.0"

libraryDependencies ++= Seq(
  "mysql" % "mysql-connector-java" % "5.1.+",
  "org.scalikejdbc" %% "scalikejdbc" % "2.4.+",
  "org.json4s" %% "json4s-jackson" % "3.5.0",
  "com.spotify" %% "scio-core" % "0.3.0",
  "com.spotify" %% "scio-test" % "0.3.0" % "test",
  "org.apache.beam" % "beam-sdks-java-core" % beamVersion
)

addCompilerPlugin("org.scalamacros" % "paradise" % "2.1.0" cross CrossVersion.full)