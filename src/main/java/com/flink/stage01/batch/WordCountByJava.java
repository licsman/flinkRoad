/*
 * Copyright 2020, MiaoJiawei All rights reserved.
 */

package com.flink.stage01.batch;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.AggregateOperator;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

public class WordCountByJava {

    /**
     * java版 word count .
     * @param args .
     */
    public static void main(String[] args) throws Exception {
        // set up the execution environment
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        String filePath = "input/text.log";
        DataSet<String> text = env.readTextFile(filePath);
        AggregateOperator<Tuple2<String, Integer>> count = text.flatMap(new Pair())
                .groupBy(0)
                .sum(1);
        count.writeAsCsv("output/java_batch/");
        env.setParallelism(1);
        env.execute("javaBatch");
    }

    public static final class Pair implements FlatMapFunction<String, Tuple2<String, Integer>> {
        /**
         * 将输入的每行数据进行切分单词并解析为二元组 .
         * @param value 每行数据 .
         * @param out 处理后的输出结果 .
         * @throws Exception .
         */
        public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
            String[] words = value.split("\\s+");
            for (String perWord : words) {
                if (perWord.length() > 0) {
                    out.collect(new Tuple2<String, Integer>(perWord, 1));
                }
            }
        }
    }
}
