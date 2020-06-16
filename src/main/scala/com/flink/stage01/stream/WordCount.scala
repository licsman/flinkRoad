package com.flink.stage01.stream

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala._

object WordCount {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val socketData: DataStream[String] = env.socketTextStream("172.20.3.37",9999)
    val count = socketData.flatMap(perLie => {
      perLie.split("\\s+")
    }).filter(_.nonEmpty)
      .map((_, 1))
      .keyBy(0)
      .sum(1)

    count.print()
    env.execute("wordCount streaming")
  }
}
