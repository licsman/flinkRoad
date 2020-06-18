package com.flink.stage01.stream.groupExample;

public class Data {
    private String machine;
    private Double value;

    public Data(String machine, Double value) {
        this.machine = machine;
        this.value = value;
    }

    public String getMachine() {
        return machine;
    }

    public Double getValue() {
        return value;
    }

    @Override
    public String toString() {
        return "Data{" +
                "machine='" + machine + '\'' +
                ", value=" + value +
                '}';
    }
}
