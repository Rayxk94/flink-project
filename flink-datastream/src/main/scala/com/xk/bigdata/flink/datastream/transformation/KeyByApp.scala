package com.xk.bigdata.flink.datastream.transformation

import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, _}

object KeyByApp {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(4)
    val stream = env.readTextFile("data/wc.txt")
    stream.flatMap(_.toLowerCase.split(","))
        .map((_,1))
        .keyBy(_._1)
        .print()
    env.execute(this.getClass.getSimpleName)
  }

}
