package com.flink.stage01.stream.consumer.model;

import lombok.Getter;

@Getter
public enum OrderType {
    RED("RED", 0), GREEN("GREEN", 1), BLACK("BLACK", 2);
    private final String name;
    private final int id;

    OrderType(String name, int id) {
        this.name = name;
        this.id = id;
    }
}
