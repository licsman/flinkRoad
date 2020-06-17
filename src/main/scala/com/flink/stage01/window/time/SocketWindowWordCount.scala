package com.flink.stage01.window.time

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time

object SocketWindowWordCount extends App {
  val hostname = "172.20.3.37"
  val port = 9999
  val env = StreamExecutionEnvironment.getExecutionEnvironment
  val data = env.socketTextStream(hostname ,port)
  val windowCount = data.flatMap(_.split("\\s+"))
    .filter(_.nonEmpty)
    .map((_,1))
    .keyBy(0)
    .timeWindow(Time.seconds(5))
    .sum(1)

  windowCount.print().setParallelism(1)
  env.execute("Socket Window WordCount")
}
