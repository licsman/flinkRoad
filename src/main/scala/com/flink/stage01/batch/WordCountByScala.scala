package com.flink.stage01.batch

import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.api.scala._

object WordCountByScala {
  def main(args: Array[String]): Unit = {
    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    val data: DataSet[String] = env.readTextFile("input/text.log")
    val res: AggregateDataSet[(String, Int)] = data.flatMap(_.split("\\s+").filter(_.nonEmpty))
      .map((_,1))
      .groupBy(0)
      .sum(1)
    res.writeAsCsv("output/01")
    env.execute("wordCount")
  }
}
