package com.xk.bigdata.flink.datastream.transformation

import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, _}

object FilterApp {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(4)
    val stream = env.readTextFile("data/wc.txt")
    stream.flatMap(_.toLowerCase.split(","))
        .map((_,1))
        .filter(_._1 == "spark")
        .print()
    env.execute(this.getClass.getSimpleName)
  }

}
