package com.dzzxjl.mammut.local

import org.apache.spark.sql.SparkSession

import java.text.SimpleDateFormat
import java.util.Locale
import scala.collection.mutable.ArrayBuffer

object TrajTest extends AbstractApp {

	val formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss", Locale.ENGLISH)

	override def run(spark: SparkSession): Unit = {
		val sc = spark.sparkContext
		sc.setLogLevel("ERROR")

		// 2 读取出租车轨迹数据
		val path = "./src/main/resources/CU9510_06_01_trajectory.txt"

		//    val path = "/Users/dzzxjl/Desktop/CU9510_06_01_trajectory.txt"
		val trajRDD = sc.textFile(path)
		val partitionNum = trajRDD.partitions.size
		val partitionLength = trajRDD.partitions.length

		println(s"trajRDD分区数目$partitionNum")
		println(partitionLength)
		println(trajRDD)
		println(trajRDD.partitions(0))
		//    print(trajRDD.reduce((x, y) => x+y))
		//    trajRDD.checkpoint()

		def scalaMethod(a: String): String = {
			return a + "hello world"
		}

		//    val forEachRDD = trajRDD.foreach(scalaMethod)
		println(trajRDD.collect())
		//    trajRDD.take(100).foreach(println)

		val list = new ArrayBuffer[String]()

		//    val forEachRDD = trajRDD.foreach(println(x+"hll"))

		//    trajRDD.foreach(record => {
		//      list += record
		//      println("hah")
		//    })

		//    trajRDD.foreach((x:Int)=>print(x+" "))
		trajRDD.foreach((x: String) => print(x + " "))
		trajRDD.foreach(x => println(x + "heihei"))

		println(list.toArray)
		val size = list.toArray.size
		println(s"size:$size")
		println(list(0))

		for (line <- list.toArray) {
			println(line)
		}
		//    val list = new ArrayBuffer()
		//    Rdd.foreach(record => {
		//      list += record
		//      If (list.size >= 10000) {
		//        list.flush....
		//      }
		//    })

		//    println(forEachRDD.getClass.getSimpleName)
		//    println(forEachRDD)


		//    val rdd = sc.parallelize(List("hello", "world", "hello"))
		//    val rddMap = rdd.map((_,1))
		//    for (arg <- rddMap.collect())
		//      println(arg)


		//    val rdd = sc.parallelize(List(1,2,3,4,5,6)).map(_*3)
		//    val mappedRDD = rdd.filter(_>10).collect()
		//    //对集合求和
		//    println(rdd.reduce(_+_))
		//    //输出大于10的元素
		//    for(arg <- mappedRDD)
		//      print(arg+" ")
		//    println()
		//    println("math is work")


		//    val l = sc.parallelize(List(1,2,3,4))
		//    println("分区数目", l.partitions.size)
		//    val lReduce = l.reduce((x, y) => x + y)
		//    println(lReduce)
		//
		//    val lFold = l.fold(2)((x, y) => x + y)
		//    println(lFold)
		//
		//    val lAggregate = l.aggregate((0, 0))(
		//      (x, y) => (x._1 + y, x._2+1),  // x代表一个元组，即返回的类型，y代表迭代过程中的元素
		//      (x, y) => (x._1 + y._1, x._2 + y._2)  // 在每个分区上进行操作
		//    )
		//    println(lAggregate)

	}
}
