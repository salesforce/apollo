package test;

import java.sql.*;
import com.salesforce.apollo.h2.*;

public class DbAccess {
    
    public ResultSet call(Connection connection) throws Exception {
        Statement statement = connection.createStatement();
        return statement.executeQuery("select * from s.books");
    }

    public ResultSet callWitServices(Connection connection, SessionServices services) throws Exception {
        Statement statement = connection.createStatement();
        services.call("foo", "hello world");
        return statement.executeQuery("select * from s.books");
    }
}
