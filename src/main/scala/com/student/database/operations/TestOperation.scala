package com.student.database.operations

import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession

object TestOperation {
  def checkSpark(spark:SparkSession,logger:Logger):Unit = {
    import spark.implicits._
    val ds = spark.sparkContext.parallelize(List(1,2,3,4)).toDS()
    ds.show()
    logger.info("test operation executed successfully.")
  }
}
