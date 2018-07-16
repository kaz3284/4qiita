name := "dataflow"

version := "0.1.0"

scalaVersion := "2.12.4"

val beamVersion = "2.4.0"

libraryDependencies ++= Seq(
  "mysql" % "mysql-connector-java" % "5.1.+",
  "org.scalikejdbc" %% "scalikejdbc" % "2.5.+",
  "org.json4s" %% "json4s-jackson" % "3.5.0",
  "com.spotify" %% "scio-core" % "0.5.2",
  "org.apache.beam" % "beam-sdks-java-core" % beamVersion,
  "org.apache.beam" % "beam-sdks-java-io-jdbc" % beamVersion,
  "org.apache.beam" % "beam-runners-direct-java" % beamVersion,
  "org.apache.beam" % "beam-runners-google-cloud-dataflow-java" % beamVersion
)

addCompilerPlugin("org.scalamacros" % "paradise" % "2.1.0" cross CrossVersion.full)

lazy val assemblySettings = Seq(
  test in assembly := {},
  assemblyMergeStrategy in assembly ~= { old => {
    case PathList(ps@_*) if ps.last endsWith ".conf" => MergeStrategy.concat
    case PathList(ps@_*) if ps.last endsWith ".xml" => MergeStrategy.first
    case s if s.endsWith(".properties") => MergeStrategy.filterDistinctLines
    case s if s.endsWith("pom.xml") => MergeStrategy.last
    case s if s.endsWith(".class") => MergeStrategy.last
    case s if s.endsWith(".proto") => MergeStrategy.last
    case s if s.endsWith("libjansi.jnilib") => MergeStrategy.last
    case s if s.endsWith("jansi.dll") => MergeStrategy.rename
    case s if s.endsWith("libjansi.so") => MergeStrategy.rename
    case s if s.endsWith("libsnappyjava.jnilib") => MergeStrategy.last
    case s if s.endsWith("libsnappyjava.so") => MergeStrategy.last
    case s if s.endsWith("snappyjava_snappy.dll") => MergeStrategy.last
    case s if s.endsWith(".dtd") => MergeStrategy.rename
    case s if s.endsWith(".xsd") => MergeStrategy.rename
    case PathList("META-INF", "services", "org.apache.hadoop.fs.FileSystem") => MergeStrategy.filterDistinctLines
    case s => old(s)
  }
  }
)
