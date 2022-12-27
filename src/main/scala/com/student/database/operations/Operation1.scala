package com.student.database.operations

import data.constants.CassandraConstants._
import data.constants.OperationConstants._
import data.models._
import org.apache.log4j.Logger
import org.apache.spark.sql._

import scala.annotation.tailrec
import scala.util._
import scala.io.StdIn.readInt

object Operation1 {

  @tailrec
  def executeOperation1(
                         spark:SparkSession,
                         logger: Logger,
                         subjects:Map[Int,String],
                         classes:Map[Int,String],
                         groups:Map[Int,String],
                         subjectsGroupMapper:Map[Int,List[Int]],
                         classesGroupMapper:Map[Int,List[Int]]): Unit
  = {
    //get students details and subjects related to the group ID
    val studentData = getStudent(spark,logger)
    val subjectList = subjectsGroupMapper.getOrElse(studentData.group_id.getOrElse(0),List.empty)
    val mathSubjectId = subjects.find(x => x._2 == subjectMath).get._1

    //check for student data then display and repeat operation based on conditions
    if(studentData.student_id.getOrElse(0) == 0) executeOperation1(spark,logger,subjects,classes,groups,subjectsGroupMapper,classesGroupMapper)
    else if (!subjectList.contains(mathSubjectId)){
      logger.info(s"Student :${studentData.firstname.getOrElse("")} with ID: ${studentData.student_id.getOrElse(0)} data viewed.")
      displayStudent(studentData,groups,subjects,classes,subjectsGroupMapper)
      println("Please enter student ID having subject Math.")
    }
    else {
      displayStudent(studentData,groups,subjects,classes,subjectsGroupMapper)
      val avgMath = getMathTestAvgScore(spark,logger,studentData.student_id.get,mathSubjectId,studentData.class_id.get,studentData.group_id.get)
      println(s"Average of Math test score: $avgMath")
    }
  }

  //utility functions
  @tailrec
  private def getStudent(spark: SparkSession,logger: Logger,id: Int = getStudentId): Student = {
    import spark.implicits._

    val readData = spark.read.format("org.apache.spark.sql.cassandra")
      .options(Map("keyspace" -> keySpaceName, "table" -> studentsTable))
      .load
      .where(s"student_id=$id")
      .as[Student]
      .take(1)

    if(readData.length == 0) {
      logger.info(s"Student with ID: $id not found!, please try with a current year student ID.")
      getStudent(spark,logger)
    }
    else readData(0)
  }

  private def getMathTestAvgScore(spark: SparkSession, logger: Logger, id: Int,mathSubjectId:Int,classId:Int,groupId:Int):Double  = {
    import spark.implicits._

    val readData = spark.read.format("org.apache.spark.sql.cassandra")
      .options(Map("keyspace" -> "assignment2", "table" -> "marks"))
      .load
      .where(s"group_id=$groupId and class_id=$classId and subject_id=$mathSubjectId and student_id=$id")
      .as[Mark].take(1)

    println(readData(0).test_marks.getOrElse(Map()))

    if (readData.length == 0) {
      logger.info(s"Student with ID: $id, has no Math test score in data.")
      0.0
    }
    else {
      val marks = readData(0).test_marks.get.map(data => data._2.getOrElse(0.0))
      marks.sum / marks.size
    }
  }

  private def displayStudent(data:Student,groups:Map[Int,String],subjects:Map[Int,String],classes: Map[Int,String],subjectsGroupMapper:Map[Int,List[Int]]): Unit={
    println(s"name: ${data.firstname.getOrElse("").capitalize} ${data.lastname.getOrElse("")}")
    println(s"Group: ${groups.getOrElse(data.group_id.get,"")}")
    println(s"Class: ${classes.getOrElse(data.class_id.get,"")}")
    displaySubjects(data.group_id.getOrElse(0),subjectsGroupMapper:Map[Int,List[Int]],subjects:Map[Int,String])
  }

  def displaySubjects(groupId:Int,subjectsGroupMapper:Map[Int,List[Int]],subjects:Map[Int,String]):Unit = {
    val subjectsList = subjectsGroupMapper.getOrElse(groupId,List.empty).map(subjectId => subjects.get(subjectId))
    println("Subjects: ")
    subjectsList.foreach(data => println("->"+data.getOrElse("")))
    println()
  }

  @tailrec
  private def getStudentId: Int = {
    print("Student ID: ")
    val pinCode = Try(readInt())
    pinCode match {
      case Success(value) if value>=1 && value<=900 => value
      case Success(value) if !value.toString.matches("^[1-9][0-9]{5}$") =>
        println("Please enter an ID between 1 to 900!")
        getStudentId
      case Failure(exception) =>
        println("Please enter numbers only!")
        getStudentId
    }
  }
}
