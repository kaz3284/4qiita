name := "pegasus"

version := "0.0.1"

scalaVersion := "2.11.8"

libraryDependencies ++= Seq(
  "mysql" % "mysql-connector-java" % "5.1.+",
  "com.google.cloud.dataflow" % "google-cloud-dataflow-java-sdk-all" % "1.9.0",
  "org.scalikejdbc" %% "scalikejdbc" % "2.4.+",
  "org.json4s" %% "json4s-jackson" % "3.5.0",
  "com.spotify" %% "scio-core" % "0.2.13",
  "com.spotify" %% "scio-test" % "0.2.13" % "test"
)

addCompilerPlugin("org.scalamacros" % "paradise" % "2.1.0" cross CrossVersion.full)