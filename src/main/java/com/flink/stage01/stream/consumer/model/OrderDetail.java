package com.flink.stage01.stream.consumer.model;

import lombok.Data;
import java.io.Serializable;

@Data
public class OrderDetail implements Serializable {
    private int orderId;
    private int orderType;
    private int productId;
    private int price;
    private long orderTime;
}
