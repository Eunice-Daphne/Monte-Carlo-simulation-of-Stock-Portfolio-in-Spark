name := "firstsample"

version := "0.1"

scalaVersion := "2.11.11"
//scalaVersion := "2.10.8"


libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "2.4.0",
  "org.apache.spark" %% "spark-sql" % "2.4.0",
  "org.apache.spark" %% "spark-hive" % "2.4.0",
  //"org.apache.spark" %% "spark-mllib" % "2.4.0" ,
  //"org.apache.spark" %% "spark-streaming" % sparkVersion,
  //"org.apache.spark" %% "spark-streaming-twitter" % sparkVersion
  "com.typesafe" % "config" % "1.3.2",
  "com.typesafe.scala-logging" %% "scala-logging" % "3.9.0",
  "org.scalatest" %% "scalatest" % "3.0.5" % "test"

)


assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs@_*) => MergeStrategy.discard
  case _ => MergeStrategy.first
}

assemblyJarName in assembly := "EuniceDaphneHW3.jar"
mainClass in(Compile, run) := Some("Main")
mainClass in assembly := Some("Main")
