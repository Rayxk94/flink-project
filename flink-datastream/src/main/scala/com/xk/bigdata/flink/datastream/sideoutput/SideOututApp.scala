package com.xk.bigdata.flink.datastream.sideoutput

import com.xk.bigdata.flink.datastream.domain.City
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.streaming.api.scala.{OutputTag, StreamExecutionEnvironment, _}
import org.apache.flink.util.Collector


object SideOututApp {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(4)
    val stream = env.readTextFile("data/city.txt")
    val cityStream = stream.map(x => {
      val splits = x.split(",")
      City(splits(0), splits(1), splits(2))
    })
    val zhejiangTag = new OutputTag[City]("zhejiang")
    val anhuiTag = new OutputTag[City]("anhui")
    val otherTag = new OutputTag[City]("other")
    val sideStream = cityStream.process(new ProcessFunction[City, City] {
      override def processElement(value: City, ctx: ProcessFunction[City, City]#Context, out: Collector[City]): Unit = {
        if (value.province == "浙江省") {
          ctx.output(zhejiangTag, value)
        } else if (value.province == "安徽省") {
          ctx.output(anhuiTag, value)
        } else {
          ctx.output(otherTag, value)
        }
      }
    })
    val zhejiang = sideStream.getSideOutput(zhejiangTag)
    val anhui = sideStream.getSideOutput(anhuiTag)

    // 查看安徽省下面合肥市和芜湖市的数据
    val hefeiTag = new OutputTag[City]("hefei")
    val wuhuTag = new OutputTag[City]("wuhu")
    val anhuiCityStream = anhui.process(new ProcessFunction[City, City] {
      override def processElement(value: City, ctx: ProcessFunction[City, City]#Context, out: Collector[City]): Unit = {
        if (value.city == "合肥市") {
          ctx.output(hefeiTag, value)
        } else if (value.city == "芜湖市") {
          ctx.output(wuhuTag, value)
        }
      }
    })
    anhuiCityStream.getSideOutput[City](hefeiTag).print()
    env.execute(this.getClass.getSimpleName)
  }

}