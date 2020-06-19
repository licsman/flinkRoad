package com.flink.stage01.stream.groupExample;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.kafka.clients.producer.ProducerRecord;

import javax.annotation.Nullable;

import java.util.Properties;

import static java.util.concurrent.TimeUnit.MILLISECONDS;

public class GroupedTimeWindow {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        String hostname = "172.20.3.37";
        int port = 9999;
        DataStream<String> stream = env.socketTextStream(hostname, port);
        SingleOutputStreamOperator<Data> originData = stream.filter(new Filter()).map(new DataMap());

        SingleOutputStreamOperator<Tuple2<String, Integer>> res = originData.filter(new FilterEx())
                .map(new TransData())
                .keyBy(0)
                .timeWindow(Time.of(10000, MILLISECONDS))
                .reduce(new ReduceReducer());

        res.addSink(new SinkToMysql<Tuple2<String, Integer>>());

        String topic = "hadoop";
        Properties producerConfig = new Properties();
        producerConfig.setProperty("bootstrap.servers","172.20.3.37:9092");
        res.addSink(new FlinkKafkaProducer<Tuple2<String, Integer>>(topic, new KafkaSerialize(topic), producerConfig, FlinkKafkaProducer.Semantic.EXACTLY_ONCE));

        env.execute("sss");
    }

    public static class Filter implements FilterFunction<String> {
        public boolean filter(String value) throws Exception {
            if (value.contains(",") && value.trim().length() > 2){
                return (value.split(",")).length == 2;
            } else return false;
        }
    }

    private static class DataMap implements MapFunction<String, Data> {
        public Data map(String value) throws Exception {
            String[] mp = value.split(",");
            return new Data(mp[0], Double.valueOf(mp[1]));
        }
    }

    private static class FilterEx implements FilterFunction<Data> {
        public boolean filter(Data value) throws Exception {
            return value.getValue() > 36.5;
        }
    }

    private static class TransData implements MapFunction<Data, Tuple2<String, Integer>> {
        public Tuple2<String, Integer> map(Data value) throws Exception {
            return new Tuple2<String, Integer>(value.getMachine(), 1);
        }
    }

    private static class SelectKey implements KeySelector<Data, String>{
        public String getKey(Data value) throws Exception {
            return value.getMachine();
        }
    }

    private static class ReduceReducer implements ReduceFunction<Tuple2<String, Integer>> {
        public Tuple2<String, Integer> reduce(Tuple2<String, Integer> value1, Tuple2<String, Integer> value2) throws Exception {
            return new Tuple2<String, Integer>(value1.f0, value1.f1 + value2.f1);
        }
    }

    private static class KafkaSerialize implements KafkaSerializationSchema<Tuple2<String, Integer>>{
        private final String kafkaTopic;
        public KafkaSerialize(String kafkaTopic) {
            this.kafkaTopic = kafkaTopic;
        }
        private static final long serialVersionUID = 1L;
        public ProducerRecord<byte[], byte[]> serialize(Tuple2<String, Integer> input, @Nullable Long aLong) {
            return new ProducerRecord<byte[], byte[]>(kafkaTopic, (input.f0 + "," + input.f1).getBytes());
        }
    }

}
