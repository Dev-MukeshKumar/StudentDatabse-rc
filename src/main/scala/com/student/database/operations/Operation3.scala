package com.student.database.operations

import data.models._
import data.constants.CassandraConstants._
import data.constants.OperationConstants._
import org.apache.log4j.Logger
import org.apache.spark.sql.{Dataset, SparkSession, functions}

object Operation3 {

  def executeOperation3(
                         spark: SparkSession,
                         logger: Logger,
                         subjects: Map[Int, String],
                         classes: Map[Int, String],
                         groups: Map[Int,String]
                       ): Unit = {


    val classIds = getIds(classes)
    val subjectIds = getIds(subjects)
    val groupIds = getIds(groups)

    val passPercentage = calculatePassPercentage(spark, logger,classIds,subjectIds,groupIds)

    passPercentage.show()

    passPercentage.write.format("org.apache.spark.sql.cassandra")
      .options(Map("keyspace" -> keySpaceName, "table" -> passPercentageTable))
      .mode("append")
      .save()
  }

  def calculatePassPercentage(spark:SparkSession,logger: Logger,classIds:String,subjectIds:String,groupIds:String):Dataset[PassPercentage] = {
    val marksListAverageMark = getMarks(spark, logger,classIds,subjectIds,groupIds)

    import spark.implicits._

    val studentCount = marksListAverageMark.groupByKey(x=>(x.subject_id,x.class_id))
      .agg(functions.count("student_id").as("student_count").as[Int])
      .select($"key._1".as("subject_id").as[Int],$"key._2".as("class_id").as[Int],$"student_count".as[Int])
      .as[StudentCountSubjectClassIdKey]

    val studentPassCount = marksListAverageMark.filter(data => data.average > passAverage)
      .groupByKey(x => (x.subject_id, x.class_id))
      .agg(functions.count("student_id").as("student_pass").as[Int])
      .select($"key._1".as("subject_id").as[Int],$"key._2".as("class_id").as[Int],$"student_pass".as[Int])
      .as[StudentPassSubjectClassIdKey]

    val passPercentageCalculated = studentCount
      .joinWith(studentPassCount,
        studentCount("class_id")===studentPassCount("class_id") &&
          studentCount("subject_id")===studentPassCount("subject_id"))
      .select($"_2.class_id".as[Int],$"_2.subject_id".as[Int],($"_2.student_pass"/$"_1.student_count"*100).as("pass_percentage").as[Double])
      .as[PassPercentage]

    passPercentageCalculated
  }

  def getMarks(spark: SparkSession,logger: Logger,classIds:String,subjectIds:String,groupIds:String):Dataset[AverageMark] = {
    import spark.implicits._
    val readData = spark.read.format("org.apache.spark.sql.cassandra")
      .options(Map("keyspace" -> keySpaceName , "table" -> marksTable))
      .load
      .where(s"group_id in ($groupIds) and class_id in ($classIds) and subject_id in($subjectIds)")
      .as[Mark]

    readData.map{
      case Mark(groupId,classId,subjectId,studentId,Some(marks)) => AverageMark(groupId, classId, subjectId, studentId, marks.values.map(value => value.getOrElse(0.0)).sum/marks.values.size)
    }
  }

  private def getIds(data: Map[Int, String]) = {
    val stringedData = data.keys.toList.toString()
    stringedData.substring(5,stringedData.length-1)
  }

}
