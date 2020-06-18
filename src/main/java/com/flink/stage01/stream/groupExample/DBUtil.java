package com.flink.stage01.stream.groupExample;

import java.sql.*;

public class DBUtil {
    private final String myDriver;
    private final String myUrl;
    private final String myUser;
    private final String myPwd;

    public DBUtil(String myDriver, String myUrl, String myUser, String myPwd) {
        this.myDriver = myDriver;
        this.myUrl = myUrl;
        this.myUser = myUser;
        this.myPwd = myPwd;
    }

    public Connection getConnection() throws SQLException {
        try {
            Class.forName(myDriver);
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
        return DriverManager.getConnection(myUrl, myUser, myPwd);
    }

    public void closeAll(Connection connection, Statement statement, ResultSet set) {

        if (connection != null) {
            try {
                connection.close();
            } catch (SQLException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
        }
        if (statement != null) {
            try {
                statement.close();
            } catch (SQLException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
        }
    }
}
