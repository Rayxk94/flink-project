package com.xk.bigdata.flink.datastream.datasource.buildin

import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, _}

/**
 * Flink Build-in Read Socket DataSource
 */
object SocketDataSourceApp {

  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    // 设置 env 的并行度
    env.setParallelism(4)
    val socketStream = env.socketTextStream("bigdata", 16666)
    // 得到 socketStream 的并行度
    println(socketStream.parallelism)
    val mapStream = socketStream.map(x => x)
    // 得到 mapStream 的并行度
    println(mapStream.parallelism)
    mapStream.print()
    env.execute(this.getClass.getSimpleName)
  }

}
