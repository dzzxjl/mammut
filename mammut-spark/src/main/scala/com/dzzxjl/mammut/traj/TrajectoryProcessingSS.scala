package com.dzzxjl.mammut.traj

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

object TrajectoryProcessingSS {
  def main(args: Array[String]): Unit = {

    /*
    Streamingtext下操作文件应注意以下几点：
    1.监控目录下的文件应该具有统一的数据格式，避免在内部解析时报错。
    2.文件必须是在监控目录下创建，可以通过原子性的移动或重命名操作，放入目录。
    3.一旦移入目录，文件就不能再修改了，如果文件是持续写入的话，新的数据是无法读取的。
    */
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)

    val sparkConf = new SparkConf().setAppName("TaxiTrajectoryStreamingFile").setMaster("local[2]")
    //2为核数
    //setMaster("spark://192.168.71.129:7077") //提交jar以集群运行使用此设置
    val ssc = new StreamingContext(sparkConf, Seconds(5)) //每隔20秒监听一次

    //		val lines = ssc.textFileStream("/home/hduser/Streamingtext")
    val lines = ssc.textFileStream("/Users/dzzxjl/SStest/")
    //val lines = ssc.textFileStream("hdfs://node01:9000/streamingdata") //手动put上传HDFS


    lines.foreachRDD { rdd =>
      val aRDD = rdd.filter(line => line == "a")
      println(aRDD)
    }

    val words = lines.flatMap(_.split(" ")) //每行数据以空格切分
    val wordcounts = words.map(x => (x, 1)).reduceByKey(_ + _)
    //		words.saveAsTextFiles("/home/wj/workspace/SparkScala/errlogs/", "txt")
    wordcounts.print()


    // 开始计算
    ssc.start()
    // 等待计算完成
    ssc.awaitTermination()
  }
}
