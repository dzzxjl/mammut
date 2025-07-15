package com.dzzxjl.mammut.lang

object OptionTest {

  def main(args: Array[String]): Unit = {

    val opt = Option(5)
    println(opt.isDefined)
    println(opt.getOrElse())
    val opt1 = None
    println(opt1.getOrElse(5))

  }
}
