package com.flink.stage01.stream.consumer;

import com.flink.stage01.stream.consumer.model.OrderDetail;
import com.sun.jmx.snmp.Timestamp;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import static java.lang.Thread.sleep;

public class ProduceOrderDetail implements SourceFunction<OrderDetail> {
    private volatile boolean isRunning = true;
    private int count = 0;

    @Override
    public void run(SourceContext<OrderDetail> ctx) throws Exception {
        while (isRunning && count < 1000) {
            OrderDetail orderDetail = new OrderDetail();
            orderDetail.setOrderId(count);
            orderDetail.setOrderType(count % 3);
            orderDetail.setPrice(Double.valueOf((Math.random()*100)).intValue());
            orderDetail.setProductId(count % 8);
            orderDetail.setOrderTime(new Timestamp(System.currentTimeMillis()).getDateTime());
            ctx.collect(orderDetail);
            System.out.println("生产第" + (count + 1) + "条：" + orderDetail.toString());
            sleep(1000);
            count ++;
        }
    }

    @Override
    public void cancel() {
        isRunning = false;
    }
}
