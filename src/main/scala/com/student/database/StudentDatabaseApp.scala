package com.student.database

import com.student.database.operations.Operation1.executeOperation1
import com.student.database.operations.Operation2.executeOperation2
import com.student.database.operations.Operation3.executeOperation3
import com.student.database.operations.Operation4.executeOperation4
import com.student.database.operations.Operation5.executeOperation5
import com.student.database.operations.Operation6.executeOperation6
import data.models._
import data.constants.CassandraConstants._
import org.apache.log4j.Logger
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

import java.util.Properties
import scala.annotation.tailrec
import scala.io.Source
import scala.util.{Success, Try}
import scala.io.StdIn.{readInt, readLine}

object StudentDatabaseApp extends Serializable {

  @transient lazy val logger:Logger = Logger.getLogger(getClass.getName)

  def main(args: Array[String]): Unit = {

    logger.info("Started Student database App!")

    //spark session creation
    val spark = SparkSession.builder()
      .config(getSparkConf)
      .getOrCreate()

    import spark.implicits._

    //read meta tables data and create a map with id and value pair
    val subjects = spark.read.format("org.apache.spark.sql.cassandra")
      .options(Map("keyspace" -> keySpaceName, "table" -> subjectsTable))
      .load
      .as[Subject]
      .map(data => (data.subject_id.getOrElse(0), data.subject_name.getOrElse(""))).collect().toMap

    val groups = spark.read.format("org.apache.spark.sql.cassandra")
      .options(Map("keyspace" -> keySpaceName, "table" -> groupsTable))
      .load
      .as[Group]
      .map(data => (data.group_id.getOrElse(0), data.group_name.getOrElse(""))).collect().toMap

    val tests = spark.read.format("org.apache.spark.sql.cassandra")
      .options(Map("keyspace" -> keySpaceName, "table" -> testsTable))
      .load
      .as[Test]
      .map(data => (data.test_id.getOrElse(0), data.test_name.getOrElse(""))).collect().toMap

    val subjectsGroupMapperRead = spark.read.format("org.apache.spark.sql.cassandra")
      .options(Map("keyspace" -> keySpaceName, "table" -> subjectGroupMapperTable))
      .load
      .as[SubjectGroupMapper].cache()

    val classGroupMapperRead = spark.read.format("org.apache.spark.sql.cassandra")
      .options(Map("keyspace" -> keySpaceName, "table" -> classGroupMapperTable))
      .load
      .as[ClassGroupMapper].cache()

    val classes = spark.read.format("org.apache.spark.sql.cassandra")
      .options(Map("keyspace" -> keySpaceName, "table" -> classesTable))
      .load
      .as[Class]
      .map(data => (data.class_id.getOrElse(0), data.class_name.getOrElse(""))).collect().toMap

    val subjectsGroupMapper = mapGroupAndSubjectMapper(subjectsGroupMapperRead.collect())

    val classesGroupMapper = mapClassAndGroupMapper(classGroupMapperRead.collect())

    //un-persist data
    classGroupMapperRead.unpersist()
    subjectsGroupMapperRead.unpersist()

    //start the operations menu
    menu(Success(None), spark, logger, subjects, classes, groups, tests, subjectsGroupMapper, classesGroupMapper)
  }

  @tailrec
  def menu(choice: Try[Option[Int]] = Success(None),sparkSession: SparkSession,logger: Logger,subjects:Map[Int,String],classes:Map[Int,String],groups:Map[Int,String],tests:Map[Int,String],subjectsGroupMapper:Map[Int,List[Int]],classesGroupMapper:Map[Int,List[Int]]): Unit = choice match {
      case Success(None) => {
        logger.info("menu printed")
        println("---------------------------Students Database App---------------------------")
        println("1. Calculate average mark in maths of a Student.")
        println("2. Display the statistics of a subject in a class.")
        println("3. Calculate pass percentage.")
        println("4. Get the top 3 students in test 1 and test 2.")
        println("5. Move a student data to history table.")
        println("6. Add grace mark of 5 to class XII students in physics.")
        println("\nNote: enter -1 to exit application!")
        println("----------------------------------------------------------------------------------")
        print("Enter your choice: ")
        menu(Try(Option(readInt())),sparkSession,logger,subjects,classes,groups,tests,subjectsGroupMapper,classesGroupMapper)
      }
      case Success(Some(-1)) => {
        println("Thanks for using Students Database App.")
        sparkSession.stop()
        logger.info("Stopped Student database App!")
        System.exit(0)
      }
      case Success(Some(value)) if value <= -2 || value >= 7 => {
        println("Please refer the menu and enter a valid operation number!")
        waitForPressingEnter()
        menu(choice = Success(None),sparkSession,logger,subjects,classes,groups,tests,subjectsGroupMapper,classesGroupMapper)
      }
      case Success(Some(value)) if value == 1 => {
        executeOperation1(sparkSession,logger,subjects,classes,groups,subjectsGroupMapper,classesGroupMapper)
        waitForPressingEnter()
        menu(Success(None),sparkSession,logger,subjects,classes,groups,tests,subjectsGroupMapper,classesGroupMapper)
      }
      case Success(Some(value)) if value == 2 => {
        executeOperation2(sparkSession, logger, subjects, classes, subjectsGroupMapper)
        waitForPressingEnter()
        menu(Success(None), sparkSession, logger, subjects, classes, groups, tests, subjectsGroupMapper, classesGroupMapper)
      }
      case Success(Some(value)) if value == 3 => {
        executeOperation3(sparkSession, logger,subjects,classes,groups)
        waitForPressingEnter()
        menu(Success(None), sparkSession, logger, subjects, classes, groups, tests, subjectsGroupMapper, classesGroupMapper)
      }
      case Success(Some(value)) if value == 4 => {
        executeOperation4(sparkSession, logger, classes,subjects,classesGroupMapper)
        waitForPressingEnter()
        menu(Success(None), sparkSession, logger, subjects, classes, groups, tests, subjectsGroupMapper, classesGroupMapper)
      }
      case Success(Some(value)) if value == 5 => {
        executeOperation5(sparkSession, logger, classes, groups)
        waitForPressingEnter()
        menu(Success(None), sparkSession, logger, subjects, classes, groups, tests, subjectsGroupMapper, classesGroupMapper)
      }
      case Success(Some(value)) if value == 6 => {
        executeOperation6(sparkSession, logger, subjects, classes, subjectsGroupMapper)
        waitForPressingEnter()
        menu(Success(None), sparkSession, logger, subjects, classes, groups, tests, subjectsGroupMapper, classesGroupMapper)
      }
      case _ => {
        println("\nEnter a valid number!")
        menu(choice = Success(None),sparkSession,logger,subjects,classes,groups,tests,subjectsGroupMapper,classesGroupMapper)
      }
  }


  //utility functions
  def waitForPressingEnter(): Unit = {
    println("\npress enter to continue.")
    readLine()
  }

  def getSparkConf(): SparkConf = {
    val sparkConf = new SparkConf
    val props = new Properties
    props.load(Source.fromFile("spark.conf").reader())
    props.forEach((k, v) => sparkConf.set(k.toString, v.toString))
    sparkConf
  }

  def mapGroupAndSubjectMapper(data: Array[SubjectGroupMapper], i: Int = 0, result: Map[Int, List[Int]] = Map()): Map[Int, List[Int]] = {
    if (i >= data.length) result
    else {
      val groupId = data(i).group_id.getOrElse(0)
      if (result.contains(groupId)) {
        val dataList = result.get(groupId).getOrElse(List.empty) :+ data(i).subject_id.getOrElse(0)
        mapGroupAndSubjectMapper(data, i + 1, result + (groupId -> dataList))
      }
      else mapGroupAndSubjectMapper(data, i + 1, result + (groupId -> List(data(i).subject_id.getOrElse(0))))
    }
  }

  def mapClassAndGroupMapper(data: Array[ClassGroupMapper], i: Int = 0, result: Map[Int, List[Int]] = Map()): Map[Int, List[Int]] = {
    if (i >= data.length) result
    else {
      val classId = data(i).class_id.getOrElse(0)
      if (result.contains(classId)) {
        val dataList = result.get(classId).getOrElse(List.empty) :+ data(i).group_id.getOrElse(0)
        mapClassAndGroupMapper(data, i + 1, result + (classId -> dataList))
      }
      else mapClassAndGroupMapper(data, i + 1, result + (classId -> List(data(i).group_id.getOrElse(0))))
    }
  }
}
