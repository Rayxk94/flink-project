package com.xk.bigdata.flink.datastream.transformation

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala._

object MapApp {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(4)
    val stream = env.readTextFile("data/wc.txt")
    stream.map(_.toLowerCase)
        .print()
    env.execute(this.getClass.getSimpleName)
  }

}
