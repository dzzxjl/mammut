package com.dzzxjl.mammut.localtest

import GroupByKeyTest.excute
import org.apache.spark.sql.SparkSession

abstract class AbstractApp {

  protected def init(appName: String): SparkSession = {
    // 本机的spark就用local，远端的就写ip
    // 如果是打成jar包运行则需要去掉 setMaster("local")因为在参数中会指定。
    val spark = SparkSession.builder()
      .appName(appName)
      .master("local[*]")
      .getOrCreate()

    // spark.sparkContext.setLogLevel("WARN")
    spark.sparkContext.setLogLevel("ERROR")

    spark
  }

  def run(spark: SparkSession): Unit

  final def excute(appName: String): Unit = {
    val spark = init(appName)

    try {
      run(spark)
    } finally {
      spark.stop()
    }

  }

  def main(args: Array[String]): Unit = {
    excute("Spark App")
  }



}
