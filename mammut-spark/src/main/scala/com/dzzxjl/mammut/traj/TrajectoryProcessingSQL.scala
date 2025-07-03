package com.dzzxjl.mammut.traj

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{StringType, StructField, StructType}

object TrajectoryProcessingSQL {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("Taxi")
      .master("local[*]")
      .getOrCreate()


    //为读取的数据创建schema
    //		CU9510,0,2013-06-01 00:00:01,31.111267,121.374267,0
    val taxiSchema = StructType(Array(
      StructField("id", StringType, true),
      StructField("status", StringType, true),
      StructField("time", StringType, true),
      StructField("lat", StringType, true),
      StructField("lon", StringType, true),
      StructField("speed", StringType, true)

    ))

    val path = "./src/main/resources/CU9510_06_01_trajectory.txt"
    //		val data = spark.read.schema(taxiSchema).csv(path)
    //		val data = spark.read.csv(path)
    val data = spark.read.format("csv").option("header", "true").load(path)
    data.show()


  }
}
