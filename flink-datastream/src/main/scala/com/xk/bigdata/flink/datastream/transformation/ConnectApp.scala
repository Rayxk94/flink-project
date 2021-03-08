package com.xk.bigdata.flink.datastream.transformation

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala._

object ConnectApp {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(4)
    val stream1 = env.readTextFile("data/wc.txt")
    val stream2 = env.readTextFile("data/wc.txt").map(("test",_))
    stream1.connect(stream2)
      .map(x=>x,y=>y)
        .print()
    env.execute(this.getClass.getSimpleName)
  }

}
