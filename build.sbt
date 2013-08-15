name := """cassandra-actor"""

version := "0.0.1-SNAPSHOT"

scalaVersion := "2.10.2"

libraryDependencies ++= Seq(
  "com.typesafe.akka" %% "akka-actor" % "2.1.4",
  "org.apache.cassandra" % "cassandra-all" % "1.2.5"
)
