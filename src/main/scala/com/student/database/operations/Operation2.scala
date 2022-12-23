package com.student.database.operations


import data.generate.MetaTable.subjectsList
import data.models._
import data.constants.Constants._
import org.apache.log4j.Logger
import org.apache.spark.sql._

import scala.annotation.tailrec
import scala.util.{Failure, Success, Try}
import scala.io.StdIn.readLine

object Operation2 {

  def executeOperation2(
                         spark: SparkSession,
                         logger: Logger,
                         subjects: Map[Int, String],
                         classes: Map[Int, String],
                         subjectsGroupMapper: Map[Int, List[Int]]): Unit = {

    val subjectName = getSubjectName()
    val className = getClassName()

    val subjectId = subjects.find(_._2 == subjectName).getOrElse((0,""))._1
    val classId = classes.find(_._2 == className).getOrElse((0,""))._1
    val groupsId = getGroupsId(subjectId,subjectsGroupMapper)

    logger.info(s"viewing data of class: $className, of groups: $groupsId")

    val studentsData = getStudents(spark,logger,groupsId,classId).filter(data => data._1 != 0)
    println(s"Students data with id and name of class: $className and groups: $groupsId")
    studentsData.foreach(println)

    val marksData = getMarks(spark,logger,subjectId,classId,groupsId).cache()

    import spark.implicits._
//    val marksAverage = marksData.groupByKey(_.testId).mapGroups((k,vs) => TestMarkGroup(k,calculateMarkAverage(vs)))
    val marksAverage = marksData.groupByKey(_.testId).agg(functions.avg($"mark").as("mark").as[Double]).select($"key".as("testId").as[Int],$"mark".as[Double]).as[TestMarkGroup]
    val arrangedAverage = marksAverage.sort($"mark".desc).take(3)

    println(s"Highest average test: ${arrangedAverage(0).testId} with average ${arrangedAverage(0).mark}")
    println(s"Lowest average test: ${arrangedAverage(2).testId} with average ${arrangedAverage(2).mark}")
    println("Percentage increase in average: "+percentageIncreaseInAverage(arrangedAverage)+"%")
    //releasing cached data
    marksData.unpersist()
  }

  private def percentageIncreaseInAverage(data:Array[TestMarkGroup]): Double = (data(0).mark - data(2).mark) /100

  private def getMarks(spark:SparkSession, logger: Logger,subjectId:Int,classId:Int,groupId: String): Dataset[TestMarkGroup] = {
    import spark.implicits._
    val readData = spark.read.format("org.apache.spark.sql.cassandra")
      .options(Map("keyspace" -> keySpaceName, "table" -> marksTable))
      .load
      .where(s"group_id in($groupId) and class_id=$classId and subject_id=$subjectId")
      .select("test_marks")
      .as[Map[Int,Double]]

    if (readData.count() == 0) {
      logger.info(s"No students marks were present at under the given subject and class")
      import spark.implicits._
      spark.emptyDataset[TestMarkGroup]
    }
    else {
      readData.flatMap( data => data.map( idata =>TestMarkGroup(idata._1,idata._2)))
    }
  }

  private def getStudents(spark: SparkSession, logger: Logger, groupId: String, classId:Int): List[(Int,String,String)] = {
    import spark.implicits._
    val readData = spark.read.format("org.apache.spark.sql.cassandra")
      .options(Map("keyspace" -> "assignment2", "table" -> "students"))
      .load
      .where(s"group_id in($groupId) and class_id=$classId")
      .select("student_id","firstname","lastname")
      .as[(Option[Int],Option[String],Option[String])].collect().toList

    if (readData.length == 0) {
      logger.info(s"No students were present at under the given subject and class")
      List.empty
    }
    else readData.map(data => (data._1.getOrElse(0),data._2.getOrElse(""),data._3.getOrElse("")))
  }

  @tailrec
  private def getSubjectName(): String = {
    println(s"Subjects ${subjectsList}")
    print("Enter a subject name: ")
    val subject = Try(readLine())
    subject match {
      case Success(value) if subjectsList.contains(value.toLowerCase.capitalize) => value.toLowerCase.capitalize
      case Success(value) if !subjectsList.contains(value.toLowerCase.capitalize) => {
        println("Please enter a valid subject!")
        getSubjectName()
      }
      case Failure(exception) => {
        println("Please enter a valid string data!")
        getSubjectName()
      }
    }
  }

  @tailrec
  private def getClassName(): String = {
    println(s"Classes list: XI, XII")
    print("Class name: ")
    val className = Try(readLine())
    className match {
      case Success(value) if value.toUpperCase == "XII" || value.toUpperCase == "XI" => value.toUpperCase
      case Success(value) => {
        println("Please enter a valid class from the list!")
        getClassName()
      }
      case Failure(exception) => {
        println("Please enter a valid string data!")
        getClassName()
      }
    }
  }

  private def getGroupsId(subjectId:Int,subjectGroupMapper: Map[Int,List[Int]]):String ={
    val groupsMap = subjectGroupMapper.filter(data => data._2.contains(subjectId))
    val groupsList = groupsMap.keys.toList
    if(groupsList.length == 1) groupsList(0).toString
    else groupsList.toString().substring(5,groupsList.toString().length-1)
  }

  private def calculateMarkAverage(data: Iterator[TestMarkGroup]): Double = {
    val dataList = data.toList
    val markSum = dataList.foldLeft(0.0)((acc,p)=> acc+p.mark)
    markSum/dataList.length
  }

}
