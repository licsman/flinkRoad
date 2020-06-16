package com.flink.stage01.stream;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class StreamWordCount {
    @SuppressWarnings("checkstyle:JavadocMethod")
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        String host = "172.20.3.37";
        int port = 9999;
        DataStreamSource<String> socketData = env.socketTextStream(host, port);
        SingleOutputStreamOperator<Tuple2<String, Integer>> count = socketData.flatMap(new TupleWord())
                .keyBy(0)
                .sum(1);
        count.print();
        env.execute("java flink steaming compute word count");
    }

    public static final class TupleWord implements FlatMapFunction<String, Tuple2<String, Integer>> {
        /**
         * 将每行数据按照空格进行分隔并以元组的形式输出.
         * @param value 输入的每行数据.
         * @param out 输出为二元组（），将每行数据分解为单词.
         * @throws Exception .
         */
        public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
            String[] words = value.split("\\s+");
            for (String word : words) {
                if (word.length() > 0) {
                    out.collect(new Tuple2<String, Integer>(word, 1));
                }
            }
        }
    }
}
