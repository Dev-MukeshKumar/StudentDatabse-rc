package com.student.database

import data.generate.MainTable._
import data.generate.MetaTable._
import data.constants.CassandraConstants._
import org.apache.log4j.Logger
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

import java.util.Properties
import scala.io.Source

object DataGenerationApp extends Serializable {
  @transient lazy val logger:Logger = Logger.getLogger(getClass.getName)

  def main(args: Array[String]): Unit = {
    logger.info("Started DummyDataApp!")

    val spark = SparkSession.builder()
      .config(getSparkConf)
      .getOrCreate()
    import spark.implicits._

    val studentsDataList = studentsTableList()
    val marksDataList = marksTableList(studentsDataList)

    //meta tables dataset generate
    val groupsDS = spark.createDataset(groups())
    val subjectsDS = spark.createDataset(subjects())
    val classesDS = spark.createDataset(classes())
    val testsDS = spark.createDataset(tests())
    val classGroupMapperDS = spark.createDataset(classGroupMapper())
    val subjectGroupMapperDS = spark.createDataset(subjectGroupMapper)

    groupsDS.write.format("org.apache.spark.sql.cassandra")
      .options(Map("keyspace" -> keySpaceName, "table" -> groupsTable))
      .mode("append")
      .save()

    subjectsDS.write.format("org.apache.spark.sql.cassandra")
      .options(Map("keyspace" -> keySpaceName, "table" -> subjectsTable))
      .mode("append")
      .save()

    classesDS.write.format("org.apache.spark.sql.cassandra")
      .options(Map("keyspace" -> keySpaceName, "table" -> classesTable))
      .mode("append")
      .save()

    testsDS.write.format("org.apache.spark.sql.cassandra")
      .options(Map("keyspace" -> keySpaceName, "table" -> testsTable))
      .mode("append")
      .save()

    classGroupMapperDS.write.format("org.apache.spark.sql.cassandra")
      .options(Map("keyspace" -> keySpaceName, "table" -> classGroupMapperTable))
      .mode("append")
      .save()

    subjectGroupMapperDS.write.format("org.apache.spark.sql.cassandra")
      .options(Map("keyspace" -> keySpaceName, "table" -> subjectGroupMapperTable))
      .mode("append")
      .save()

    val studentsDataDS = spark.createDataset(studentsDataList)
    val marksDataDS = spark.createDataset(marksDataList)

    studentsDataDS.write.format("org.apache.spark.sql.cassandra")
      .options(Map("keyspace" -> keySpaceName, "table" -> studentsTable))
      .mode("append")
      .save()

    marksDataDS.write.format("org.apache.spark.sql.cassandra")
      .options(Map("keyspace" -> keySpaceName, "table" -> marksTable))
      .mode("append")
      .save()

    spark.stop()
    logger.info("Stopped DummyDataApp!")

  }

  def getSparkConf(): SparkConf = {
    val sparkConf = new SparkConf
    val props = new Properties
    props.load(Source.fromFile("spark.conf").reader())
    props.forEach((k, v) => sparkConf.set(k.toString, v.toString))
    sparkConf
  }

}
