ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.12.10"

lazy val root = (project in file("."))
  .settings(
    name := "StudentDatabase"
  )

val sparkVersion = "3.1.2"
val sparkCassandraConnectorVersion = "3.1.0"
val jodaTimeVersion = "2.12.1"

val sparkDependencies = Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion
)

val sparkCassandraConnectorDependencies = "com.datastax.spark" %% "spark-cassandra-connector" % sparkCassandraConnectorVersion

val jodaTimeDependencies = "joda-time" % "joda-time" % jodaTimeVersion

libraryDependencies ++= sparkDependencies
libraryDependencies += sparkCassandraConnectorDependencies
libraryDependencies += jodaTimeDependencies
