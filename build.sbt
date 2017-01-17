name := "kafka-connect-mongodb"

version := "1.0"

scalaVersion := "2.11.7"

libraryDependencies ++= Seq(
  "org.apache.kafka" % "connect-json" % "0.9.0.0" % "provided",
  "org.apache.kafka" % "connect-api" % "0.9.0.0" % "provided",
  "org.mongodb" %% "casbah-core" % "3.1.1"
)

resolvers ++= Seq(
    "Artima Maven Repository" at "http://repo.artima.com/releases",
    "confluent" at "http://packages.confluent.io/maven/"
)

