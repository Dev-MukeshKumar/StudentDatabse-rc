package com.student.database.operations

import data.models._
import data.constants.CassandraConstants._
import data.constants.OperationConstants._
import org.apache.log4j.Logger
import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.sql.functions._

import scala.annotation.tailrec
import scala.util.{Failure, Success, Try}
import scala.io.StdIn.readLine

object Operation4 {
  def executeOperation4(
                         spark: SparkSession,
                         logger: Logger,
                         classes: Map[Int, String],
                         subjects: Map[Int,String],
                         classGroupMapper: Map[Int,List[Int]]
                       ): Unit = {

    val className = getClassName
    val classId = getClassId(className,classes)
    val groupIds = getIdsFromList(classGroupMapper(classId))
    val subjectIds = getIdsFromMap(subjects)
    val testWiseMarks = getMarksTestWise(spark, logger,classId,groupIds,subjectIds)

    import spark.implicits._

    val testWiseAverageMark = testWiseMarks.groupByKey(x=>(x.student_id,x.test_id))
      .agg(avg("mark").as("average_marks").as[Double])
      .cache()

    val totalAverageOfStudent = testWiseAverageMark.groupByKey(_._1._1)
      .agg(avg("average_marks").as("total_average").as[Double])

    val test1AverageMarks = testWiseAverageMark.filter(data => data._1._2 == test1).sort(col("average_marks").desc)

    val test2AverageMarks = testWiseAverageMark.filter(data => data._1._2 == test2).sort(col("average_marks").desc)

    println(s"Top 3 in test 1 and test 2 of class $className")
    test1AverageMarks.show(3)
    test2AverageMarks.show(3)

    println(s"Overall Top 3 in tests of class $className")
    totalAverageOfStudent.sort($"total_average".desc).show(3)

    testWiseAverageMark.unpersist()
  }

  def getMarksTestWise(spark: SparkSession, logger: Logger,classId:Int,groupsId:String,subjectIds:String): Dataset[MarkTestKey] = {

    import spark.implicits._
    val readData = spark.read.format("org.apache.spark.sql.cassandra")
      .options(Map("keyspace" -> keySpaceName, "table" -> marksTable))
      .load
      .where(s"group_id in ($groupsId) and class_id = $classId and subject_id in ($subjectIds)")
      .as[Mark]

    readData.flatMap(data => data match {
      case Mark(groupId, classId, subjectId, studentId, marks) => marks.get.map{
          case (key,Some(value)) => MarkTestKey(groupId.getOrElse(0),classId.getOrElse(0), subjectId.getOrElse(0), studentId.getOrElse(0), key, value)
        }
    })
  }

  @tailrec
  private def getClassName: String = {
    println(s"Classes list: XI, XII")
    print("Class name: ")
    val className = Try(readLine())
    className match {
      case Success(value) if value.toUpperCase == classXII || value.toUpperCase == classXI => value.toUpperCase
      case Success(value) =>
        println("Please enter a valid class from the list!")
        getClassName
      case Failure(exception) =>
        println("Please enter a valid string data!")
        getClassName
    }
  }

  @tailrec
  private def getClassId(className:String,classes:Map[Int,String]):Int = {
    val classId = classes.find(_._2 == className).getOrElse((0, ""))._1
    if (classId == 0) getClassId(className,classes) else classId
  }

  private def getIdsFromList(data:List[Int]):String = {
    val stringedData = data.toString()
    stringedData.substring(5,stringedData.length - 1)
  }

  private def getIdsFromMap(data: Map[Int, String]) = {
    val stringedData = data.keys.toList.toString()
    stringedData.substring(5, stringedData.length - 1)
  }

}
