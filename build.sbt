organization := "com.startapp.data"

name := "kafka-connect-mongodb"

version := "1.0.3"

scalaVersion := "2.11.7"

libraryDependencies ++= Seq(
  "org.apache.kafka" % "connect-json" % "0.9.0.0" % "provided",
  "org.apache.kafka" % "connect-api" % "0.9.0.0" % "provided",
  "org.mongodb" %% "casbah-core" % "3.1.1",
  "org.scalactic" %% "scalactic" % "3.0.1",
  "org.scalatest" %% "scalatest" % "3.0.1" % "test",
  "org.mockito" % "mockito-core" % "2.4.2" % Test
)

resolvers ++= Seq(
    "Artima Maven Repository" at "http://repo.artima.com/releases",
    "confluent" at "http://packages.confluent.io/maven/",
    "Artima Maven Repository" at "http://repo.artima.com/releases"
)

testOptions in Test += Tests.Argument(TestFrameworks.ScalaTest, "-u", "target/scalatest-reports")
