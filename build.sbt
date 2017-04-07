
//name := "Project"
//
//version := "1.0"
//
//scalaVersion := "2.12.1"
//

name := "Project"

version := "1.0"

scalaVersion := "2.10.5"

resolvers ++= Seq(
  Resolver.sonatypeRepo("releases"),
  Resolver.sonatypeRepo("snapshots")
)

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "2.1.0",
  "org.apache.spark"  %% "spark-mllib" % "2.1.0",
  "org.apache.spark"  %% "spark-graphx" % "2.1.0"
)

libraryDependencies += "org.scalatest" % "scalatest_2.10" % "2.2.1" % "test"
