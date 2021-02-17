package com.xk.bigdata.flink.basic.scala

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala._

/**
 * Scala Flink 流处理 WC
 */
object StreamingWcApp {

  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment

    env.socketTextStream("bigdata", 16666)
      .flatMap(_.toLowerCase.split(","))
      .map((_, 1))
      .keyBy(_._1)
      .sum(1)
      .print()

    env.execute(this.getClass.getSimpleName)
  }

}
