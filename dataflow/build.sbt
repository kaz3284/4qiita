name := "pegasus"

version := "0.0.1"

scalaVersion := "2.11.8"

libraryDependencies ++= Seq(
  "mysql" % "mysql-connector-java" % "5.1.+",
  "com.google.cloud.dataflow" % "google-cloud-dataflow-java-sdk-all" % "1.9.0",
  "org.scalikejdbc" %% "scalikejdbc"        % "2.4.+",
  "com.spotify" %% "scio-core" % "0.3.0",
  "com.spotify" %% "scio-test" % "0.3.0" % "test"
)

addCompilerPlugin("org.scalamacros" % "paradise" % "2.1.0" cross CrossVersion.full)

assemblyMergeStrategy in assembly := {
  case PathList(ps @ _*) if ps.last endsWith ".class" => MergeStrategy.first
  case "application.conf"                             => MergeStrategy.concat
  case "unwanted.txt"                                 => MergeStrategy.discard
  case x =>
    val oldStrategy = (assemblyMergeStrategy in assembly).value
    oldStrategy(x)
}
