package com.dzzxjl.mammut.local

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SparkSession}

/**
 * 归因模型测试
 */
object GroupByKeyTest extends AbstractApp {

  override def run(spark: SparkSession): Unit = {
    import spark.implicits._

    // val df = Seq(
    //   (1, 2, 3),
    //   (4, 5, 6)
    // ).toDF()

    val expRDD: RDD[(String, Int)] = Seq(
      ("a", 1),
      ("b", 2),
      ("a", 3),
    ).toDF().rdd
      .map{
        x => (x.getString(0), x.getInt(1))
      }

    val actionRDD = Seq(
      ("a", 111)
    ).toDF().rdd
      .map{
        x => (x.getString(0), x.getInt(1))
      }

    expRDD.join(actionRDD)
      .map{x => ((x._1, x._2._1), x._2._2)}
      .groupByKey.collect.foreach(println)
    // .map()


    expRDD.join(actionRDD)
      .groupByKey()
      .map(x => (x._1, x._2))

    // println(df)
    // df.show()

  }

}
