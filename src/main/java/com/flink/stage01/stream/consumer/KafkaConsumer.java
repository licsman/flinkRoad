package com.flink.stage01.stream.consumer;

import com.flink.stage01.stream.consumer.model.OrderDetail;
import com.flink.stage01.stream.consumer.sink.OrderDetailTableSink;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.util.Properties;

public class KafkaConsumer {

    public static void main(String[] args) throws Exception {
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "172.20.3.63:9092");
        properties.setProperty("group.id", "youtest2");
        String topic = "youtest2";

        FlinkKafkaConsumer<OrderDetail> myConsumer = new FlinkKafkaConsumer<OrderDetail>(
                topic,
                new MapDeserSchema(),
                properties
        );

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<OrderDetail> stream = env.addSource(myConsumer);

        stream.addSink(new OrderDetailTableSink());

        env.execute("kafka");
    }
}
