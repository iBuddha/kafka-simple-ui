name := """kafka-simple-ui"""

version := "1.0-SNAPSHOT"

lazy val root = (project in file(".")).enablePlugins(PlayScala)

resolvers += Resolver.mavenLocal

scalaVersion := "2.11.11"

libraryDependencies += jdbc
libraryDependencies += cache
libraryDependencies += ws
libraryDependencies += filters
libraryDependencies += "org.scalatestplus.play" %% "scalatestplus-play" % "2.0.0" % Test
libraryDependencies += "org.apache.kafka" %% "kafka" % "0.10.1.1"
libraryDependencies += "org.apache.kafka" % "kafka-clients" % "0.10.1.1"
libraryDependencies += "org.apache.curator" % "curator-test" % "2.10.0"
libraryDependencies += "org.apache.curator" % "curator-framework" % "2.10.0"
libraryDependencies += "com.typesafe.akka" %% "akka-testkit" % "2.4.18"
