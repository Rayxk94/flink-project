package com.xk.bigdata.flink.basic.scala

import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.api.scala._

/**
 * Scala Flink 批处理 WC
 */
object BatchWcApp {

  def main(args: Array[String]): Unit = {

    val env = ExecutionEnvironment.getExecutionEnvironment
    env.readTextFile("data/wc.txt")
      .flatMap(_.toLowerCase.split(","))
      .map((_, 1))
      .groupBy(0)
      .sum(1)
      .print()
  }

}
