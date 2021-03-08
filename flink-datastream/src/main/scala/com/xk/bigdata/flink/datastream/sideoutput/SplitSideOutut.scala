package com.xk.bigdata.flink.datastream.sideoutput

import com.xk.bigdata.flink.datastream.domain.City
import org.apache.flink.streaming.api.scala.{OutputTag, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.scala._

object SplitSideOutut {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(4)
    val stream = env.readTextFile("data/city.txt")
    val cityStream = stream.map(x => {
      val splits = x.split(",")
      City(splits(0), splits(1), splits(2))
    })
    val splitStream = cityStream.split(x => {
      if (x.province == "浙江省") {
        Seq("zhejiang")
      } else if (x.province == "安徽省") {
        Seq("anhui")
      } else {
        Seq("other")
      }
    })
    splitStream select "anhui" print()
    splitStream.select("zhejiang").print()
    env.execute(this.getClass.getSimpleName)
  }

}