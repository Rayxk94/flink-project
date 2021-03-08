package com.xk.bigdata.flink.datastream.sideoutput

import com.xk.bigdata.flink.datastream.domain.City
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala._

object SimpleSideOutput {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(4)
    val stream = env.readTextFile("data/city.txt")
    val cityStream = stream.map(x => {
      val splits = x.split(",")
      City(splits(0), splits(1), splits(2))
    })
    val zhejiang = cityStream.filter(_.province == "浙江省")
    val anhui = cityStream.filter(_.province == "安徽省")
    zhejiang.print()
    anhui.print()
    env.execute(this.getClass.getSimpleName)
  }

}