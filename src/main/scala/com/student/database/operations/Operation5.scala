package com.student.database.operations

import com.datastax.spark.connector.toRDDFunctions
import data.models._
import data.constants.CassandraConstants._
import org.apache.log4j.Logger
import org.apache.spark.sql.{Dataset, SparkSession}

import scala.annotation.tailrec
import scala.io.StdIn.readInt
import scala.util.{Failure, Success, Try}
import java.time.LocalDate

object Operation5 {

  def executeOperation5(
                         spark: SparkSession,
                         logger: Logger,
                         classes: Map[Int, String],
                         groups: Map[Int, String],
                       ): Unit
  = {

    val groupIds = getIds(groups)
    val classIds = getIds(classes)

    val studentToDelete = getStudent(spark,logger,classIds,groupIds)
    println("Student data to be moved to history:")
    studentToDelete.show()

    val studentData = studentToDelete.head()

    val studentId = studentData.student_id.getOrElse(0)
    val classId = studentData.class_id.getOrElse(0)
    val groupId = studentData.group_id.getOrElse(0)

    val marks = getMarks(spark,logger,classId,groupId,studentId)

    println("marks going to be moved to history:")
    marks.show()

    val time = LocalDate.now()
    import spark.implicits._
    val studentHistoryData = spark.createDataset(Seq(StudentHistory(studentData.group_id,studentData.class_id,Option(time),studentData.student_id,studentData.firstname,studentData.lastname)))

    studentHistoryData.rdd.deleteFromCassandra(keySpaceName,studentsTable)

    marks.rdd.deleteFromCassandra(keySpaceName,marksTable)

    studentHistoryData.write.format("org.apache.spark.sql.cassandra")
      .options(Map("keyspace" -> keySpaceName, "table" -> studentsHistoryTable))
      .mode("append")
      .save()

    marks.write.format("org.apache.spark.sql.cassandra")
      .options(Map("keyspace" -> keySpaceName, "table" -> marksHistoryTable))
      .mode("append")
      .save()
  }

  @tailrec
  private def getStudent(spark: SparkSession, logger: Logger, classIds:String, groupIds:String, id: Int=getStudentId): Dataset[Student] = {
    import spark.implicits._

    val readData = spark.read.format("org.apache.spark.sql.cassandra")
      .options(Map("keyspace" -> keySpaceName, "table" -> studentsTable))
      .load
      .where(s"group_id in ($groupIds) and class_id in ($classIds) and student_id=$id")
      .as[Student]

    if (readData.count() == 0) {
      logger.info(s"Student with ID: $id not found!")
      getStudent(spark,logger,classIds,groupIds,getStudentId)
    }
    else readData
  }

  @tailrec
  private def getStudentId: Int = {
    print("Student ID: ")
    val pinCode = Try(readInt())
    pinCode match {
      case Success(value) if value >= 1 && value <= 900 => value
      case Success(value) if !value.toString.matches("^[1-9][0-9]{5}$") =>
        println("Please enter an ID between 1 to 900!")
        getStudentId
      case Failure(exception) =>
        println("Please enter numbers only!")
        getStudentId
    }
  }

  def getMarks(spark: SparkSession, logger: Logger, classId: Int,groupId:Int,studentId:Int): Dataset[Mark] = {
    import spark.implicits._
    spark.read.format("org.apache.spark.sql.cassandra")
      .options(Map("keyspace" ->keySpaceName, "table" -> marksTable))
      .load
      .where(s"group_id = $groupId and class_id = $classId and student_id=$studentId")
      .as[Mark]
  }

  private def getIds(data: Map[Int, String]):String = {
    val stringedData = data.keys.toList.toString()
    stringedData.substring(5, stringedData.length - 1)
  }
}
