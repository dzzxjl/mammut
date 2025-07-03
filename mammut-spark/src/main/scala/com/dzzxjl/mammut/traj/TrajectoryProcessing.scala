package com.dzzxjl.mammut.traj

import org.apache.spark.{SparkConf, SparkContext}

object TrajectoryProcessing {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("trajectorySpark")
    conf.setMaster("local")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")

    val path = "./src/main/resources/CU9510_06_01_trajectory.txt,src/main/resources/CU9510_06_02_trajectory.txt"
    val trajRDD = sc.textFile(path)

    //		/*--------------读取HDFS数据-------------------*/
    //		val taxiRaw = sc.textFile("hdfs://master:9000/taxidata")

    println(trajRDD.collect())
    val count = trajRDD.count()
    val countDistinct = trajRDD.distinct().count()
    println(s"去重之前$count, 去重之后$countDistinct")


    //		val mapRDD = trajRDD.map(line => (line.length, 1))
    //		mapRDD.collect()
    //		println(mapRDD.take(5)(0))
    //		println(mapRDD.take(5)(1))
    //		println(mapRDD.take(5)(2))
    //
    //		val lengthCounts = trajRDD.map(line => (line.length, 1)).reduceByKey(_ + _)
    //		println(lengthCounts.take(2)(0))
    //		println(lengthCounts.take(2)(1))

    //		def parse(line: String): (String, Trip) = {
    //			val fields = line.split(',')
    //			val license = fields(1)
    //			// Not thread-safe:
    //			val formatterCopy = formatter.clone().asInstanceOf[SimpleDateFormat]
    //			val pickupTime = new DateTime(formatterCopy.parse(fields(5)))
    //			val dropoffTime = new DateTime(formatterCopy.parse(fields(6)))
    //			val pickupLoc = point(fields(10), fields(11))
    //			val dropoffLoc = point(fields(12), fields(13))
    //
    //			val trip = Trip(pickupTime, dropoffTime, pickupLoc, dropoffLoc)
    //			(license, trip)
    //		}
    //
    //		def safe[S, T](f: S => T): S => Either[T, (S, Exception)] = {
    //			new Function[S, Either[T, (S, Exception)]] with Serializable {
    //				def apply(s: S): Either[T, (S, Exception)] = {
    //					try {
    //						Left(f(s))
    //					} catch {
    //						case e: Exception => Right((s, e))
    //					}
    //				}
    //			}
    //		}


  }
}
