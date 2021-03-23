package test;

import java.sql.*; 

public class DbAccess {
    
    public ResultSet call(Connection connection) throws Exception {
        Statement statement = connection.createStatement();
        return statement.executeQuery("select * from s.books");
    }
}