package com.flink.stage01.stream.consumer;

import com.flink.stage01.stream.consumer.model.OrderDetail;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.kafka.clients.producer.ProducerRecord;

import javax.annotation.Nullable;

public class MapSerSchema implements KafkaSerializationSchema<OrderDetail> {

    @Override
    public ProducerRecord<byte[], byte[]> serialize(OrderDetail orderDetail, @Nullable Long aLong) {

        ProducerRecord<byte[], byte[]> producerRecord = new ProducerRecord(
             "youtest2",
             orderDetail.getOrderType(),
             orderDetail.getOrderId(),
             orderDetail
        );
        return producerRecord;
    }

}
