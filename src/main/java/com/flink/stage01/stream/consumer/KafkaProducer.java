package com.flink.stage01.stream.consumer;

import com.flink.stage01.stream.consumer.model.OrderDetail;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import com.flink.stage01.stream.consumer.model.OrderSerializer;
import java.util.Properties;

public class KafkaProducer {
    public static void main(String[] args) throws Exception {
        String topic = "youtest2";
        MapSerSchema mapSerSchema = new MapSerSchema();
        Properties kafkaConfig = new Properties();
        kafkaConfig.setProperty("bootstrap.servers", "172.20.3.63:9092,172.20.3.64:9092,172.20.3.65:9092");
        kafkaConfig.setProperty("key.serializer", "org.apache.kafka.common.serialization.IntegerSerializer");
        kafkaConfig.setProperty("value.serializer", OrderSerializer.class.getName());
        FlinkKafkaProducer.Semantic semantic = FlinkKafkaProducer.Semantic.EXACTLY_ONCE;
        FlinkKafkaProducer<OrderDetail> myProducer = new FlinkKafkaProducer<OrderDetail>(
                topic,
                mapSerSchema,
                kafkaConfig,
                semantic
        );

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<OrderDetail> stream = env.addSource(new ProduceOrderDetail());

        stream.addSink(myProducer);

        env.execute("kbs");
    }
}