package com.flink.stage01.stream.groupExample;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

public class SinkToMysql<T extends Tuple2<String, Integer>> extends RichSinkFunction<Tuple2<String, Integer>> implements SinkFunction<Tuple2<String, Integer>> {
    private static final long serialVersionUID = 1L;
    private Connection connection;

    @Override
    public void open(Configuration parameters) throws Exception {
        final String url = "jdbc:mysql://localhost/green?useUnicode=true&characterEncoding=utf8&useSSL=false&serverTimezone=GMT";
        final String driver = "com.mysql.cj.jdbc.Driver";
        final String user = "root";
        final String pass = "MySQL!23";
        DBUtil dbUtil = new DBUtil(driver,url,user,pass);
        connection = dbUtil.getConnection();
    }

    public void invoke(Tuple2<String, Integer> value){
        try {
            String sql = "insert into temperature(machine,number) values(?,?) ON DUPLICATE KEY UPDATE number = ?";
            PreparedStatement preparedStatement = connection.prepareStatement(sql);
            preparedStatement.setString(1,value.f0);
            preparedStatement.setInt(2,value.f1);
            preparedStatement.setInt(3,value.f1);
            preparedStatement.executeUpdate();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
}
