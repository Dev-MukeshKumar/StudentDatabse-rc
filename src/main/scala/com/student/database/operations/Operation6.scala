package com.student.database.operations

import data.models._
import data.constants.CassandraConstants._
import data.constants.OperationConstants._
import org.apache.log4j.Logger
import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.sql.functions._

object Operation6 {
  def executeOperation6(
                         spark: SparkSession,
                         logger: Logger,
                         subjects: Map[Int, String],
                         classes: Map[Int, String],
                         subjectsGroupMapper: Map[Int, List[Int]]): Unit = {

    val physicsId = subjects.find(_._2 == subjectPhysics).get._1
    val classId = classes.find(_._2 == classXII).get._1
    val groupIds = getGroupsId(physicsId,subjectsGroupMapper)

    val physicsMarksGrouped = getMarksForASubject(spark,logger,classId,groupIds,physicsId).cache()

    val test1Marks = getMarksForATest(spark,logger,classId,1,groupIds).cache()

    import spark.implicits._

    val physicsOverallAverage = physicsMarksGrouped.map {
      case Mark(Some(groupId), Some(classId), Some(subjectId), Some(studentId), Some(marks)) => StudentAverage(studentId, marks.values.map(value => value.getOrElse(0.0)).sum / marks.size)
    }.filter(data => data.average > overallAverage)

    val test1OverallAverageMarks = test1Marks.groupByKey(_.student_id).agg(avg("mark").as("average").as[Double])
      .filter(data => data._2 > test1OverallAverage)
      .map(l => StudentAverage(l._1,l._2))


    val graceMarkEligibleStudentsIdList = physicsOverallAverage.joinWith(test1OverallAverageMarks,
      physicsOverallAverage.col("student_id")===test1OverallAverageMarks.col("student_id"),"inner")
      .select($"_1.student_id".as[Int])
      .collect().toList

    val updatedPhysicsMarksGrouped = physicsMarksGrouped.map {
      case Mark(group_id, class_id, subject_id, Some(student_id), Some(test_marks)) if graceMarkEligibleStudentsIdList.contains(student_id) =>
        val testMarks = test_marks.map {
          case (key, Some(value)) if key == 1 && value < 95 => (key, Option(value + 5))
          case (key, Some(value)) if key == 1 && value <= 95 => (key, Option(100.toDouble))
          case (key, value) => (key, value)
        }
        Mark(group_id, class_id, subject_id, Option(student_id), Option(testMarks))
      case Mark(group_id, class_id, subject_id, student_id, test_marks) => Mark(group_id, class_id, subject_id, student_id, test_marks)
    }

    updatedPhysicsMarksGrouped.write.format("org.apache.spark.sql.cassandra")
      .options(Map("keyspace" -> keySpaceName, "table" -> marksTable))
      .mode("append")
      .save()

    println("Class XII Students eligible for +5 grace mark in Physics are: ")
    getStudentsName(spark,logger,classId,groupIds,graceMarkEligibleStudentsIdList) foreach println

    println("Pass percentage in Physics before grace mark: ")
    spark.read.format("org.apache.spark.sql.cassandra")
      .options(Map("keyspace" -> keySpaceName, "table" -> passPercentageTable))
      .load
      .where(s"class_id = $classId and subject_id=$physicsId")
      .as[PassPercentage].show()

    println("Pass percentage in Physics after grace mark: ")
    val classXIIPhysicsPassPercentage = Operation3.calculatePassPercentage(spark,logger,classId.toString,physicsId.toString,groupIds)

    classXIIPhysicsPassPercentage.show()

    classXIIPhysicsPassPercentage.write.format("org.apache.spark.sql.cassandra")
          .options(Map("keyspace" -> keySpaceName, "table" -> passPercentageTable))
          .mode("append")
          .save()

    physicsMarksGrouped.unpersist()
    test1Marks.unpersist()
  }

  def getMarksForASubject(spark: SparkSession, logger: Logger, classId: Int,groupIds:String,subjectId:Int): Dataset[Mark] = {
    import spark.implicits._
    spark.read.format("org.apache.spark.sql.cassandra")
      .options(Map("keyspace" -> keySpaceName, "table" -> marksTable))
      .load
      .where(s"group_id in ($groupIds) and class_id = $classId and subject_id=$subjectId")
      .as[Mark]
  }

  def getMarksForATest(spark: SparkSession, logger: Logger, classId: Int,testId:Int,groupIds:String): Dataset[MarkTestKey] = {
    import spark.implicits._
    val readData = spark.read.format("org.apache.spark.sql.cassandra")
      .options(Map("keyspace" -> keySpaceName, "table" -> marksTable))
      .load
      .where(s"group_id in ($groupIds) and class_id = $classId")
      .as[Mark]

    readData.flatMap {
      case Mark(groupId, classId, subjectId, studentId, marks) => marks.get.map{ case (key, Some(value)) => MarkTestKey(groupId.getOrElse(0), classId.getOrElse(0), subjectId.getOrElse(0), studentId.getOrElse(0), key, value) }
    }.filter(data => data.test_id == testId)
  }

  private def getGroupsId(subjectId: Int, subjectGroupMapper: Map[Int, List[Int]]): String = {
    val groupsMap = subjectGroupMapper.filter(data => data._2.contains(subjectId))
    val groupsList = groupsMap.keys.toList
    if (groupsList.length == 1) groupsList.head.toString
    else groupsList.toString().substring(5, groupsList.toString().length - 1)
  }

  private def getStudentsName(spark:SparkSession,logger: Logger,classId:Int,groupIds:String,studentsIds:List[Int]):List[String] = {
    import spark.implicits._
    val readData = spark.read.format("org.apache.spark.sql.cassandra")
      .options(Map("keyspace" -> keySpaceName, "table" -> studentsTable))
      .load
      .where(s"group_id in ($groupIds) and class_id=$classId")
      .as[Student]

    readData.filter(data => studentsIds.contains(data.student_id.getOrElse(0))).map(data => data.firstname.getOrElse("") + " " + data.lastname.getOrElse("")).collect().toList
  }
}
