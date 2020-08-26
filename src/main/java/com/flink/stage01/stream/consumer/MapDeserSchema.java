package com.flink.stage01.stream.consumer;

import com.flink.stage01.stream.consumer.model.OrderDetail;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.connectors.kafka.KafkaDeserializationSchema;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;


public class MapDeserSchema implements KafkaDeserializationSchema<OrderDetail> {

    @Override
    public boolean isEndOfStream(OrderDetail nextElement) {
        return false;
    }

    @Override
    public OrderDetail deserialize(ConsumerRecord<byte[], byte[]> message) throws Exception {
        OrderDetail orderDetail = new OrderDetail();
        if (message.value().length > 0 && message.partition() == 0) {
            ByteArrayInputStream bi = null;
            ObjectInputStream oi = null;
            try {
                bi =new ByteArrayInputStream(message.value());
                oi =new ObjectInputStream(bi);
                orderDetail = (OrderDetail) oi.readObject();
            } catch (Exception e) {
                e.printStackTrace();
            }finally {
                try {
                    if(bi!=null){
                        bi.close();
                    }
                    if(oi!=null){
                        oi.close();
                    }
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        } else {
            orderDetail = null;
        }
        return orderDetail;
    }

    @Override
    public TypeInformation<OrderDetail> getProducedType() {
        return TypeInformation.of(OrderDetail.class);
    }
}
