package com.flink.stage01.stream.consumer.sink;

import com.flink.stage01.stream.consumer.model.OrderDetail;
import com.flink.stage01.stream.groupExample.DBUtil;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

public class OrderDetailTableSink extends RichSinkFunction<OrderDetail> implements SinkFunction<OrderDetail> {
    private static final long serialVersionUID = 1L;
    private Connection connection;

    @Override
    public void open(Configuration parameters) throws Exception {
        final String url = "jdbc:mysql://localhost/order?useUnicode=true&characterEncoding=utf8&useSSL=false&serverTimezone=GMT";
        final String driver = "com.mysql.cj.jdbc.Driver";
        final String user = "root";
        final String pass = "MySQL!23";
        DBUtil dbUtil = new DBUtil(driver,url,user,pass);
        connection = dbUtil.getConnection();
    }

    @Override
    public void invoke(OrderDetail input){
        try {
            String sql = "insert into order_detail(ordertype,productid,price,ordertime) values(?,?,?,?) ON DUPLICATE KEY UPDATE orderid = ?";
            PreparedStatement preparedStatement = connection.prepareStatement(sql);
            preparedStatement.setInt(1,input.getOrderType());
            preparedStatement.setInt(2,input.getProductId());
            preparedStatement.setInt(3,input.getPrice());
            preparedStatement.setLong(4,input.getOrderTime());
            preparedStatement.setLong(5,input.getOrderId());
            preparedStatement.executeUpdate();
            System.out.println("写入：" + input.toString());
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
}
