package com.xk.bigdata.flink.datastream.transformation

import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, _}

object UnionApp {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(4)
    val stream1 = env.readTextFile("data/wc.txt")
    val stream2 = env.readTextFile("data/wc.txt")
    stream1.union(stream2)
        .print()
    env.execute(this.getClass.getSimpleName)
  }

}
