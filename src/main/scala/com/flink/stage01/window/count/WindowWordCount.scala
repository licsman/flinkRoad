package com.flink.stage01.window.count

import com.flink.stage01.util.WordCountData
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala._

object WindowWordCount extends App {
  val env = StreamExecutionEnvironment.getExecutionEnvironment
  val data: DataStream[String] = env.fromElements(WordCountData.WORDS:_*)
  val count = data.flatMap(_.toLowerCase.split("\\s+"))
    .filter(_.nonEmpty)
    .map((_,1))
      .keyBy(0)
      .countWindow(30)
      .sum(1)

  env.execute()
}
