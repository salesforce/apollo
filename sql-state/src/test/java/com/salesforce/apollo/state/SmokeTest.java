package com.salesforce.apollo.state;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;

import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

import org.apache.calcite.config.CalciteConnectionProperty;
import org.apache.calcite.server.ServerDdlExecutor;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.parser.SqlParser.Config;
import org.apache.calcite.sql.parser.ddl.SqlDdlParserImpl;
import org.apache.calcite.sql.validate.SqlConformanceEnum;
import org.junit.jupiter.api.Test;

import com.google.common.io.CharStreams;

public class SmokeTest {

    @Test
    public void smokin() throws Exception {
        Config sqlParserConfig = SqlParser.configBuilder()
                                          .setParserFactory(SqlDdlParserImpl.FACTORY)
                                          .setConformance(SqlConformanceEnum.BABEL)
                                          .build();

        InputStream is = getClass().getResourceAsStream("/sql/smoke.sql");
        String sql = null;
        try (Reader reader = new InputStreamReader(is)) {
            sql = CharStreams.toString(reader);
        }

        SqlParser parser = SqlParser.create(sql, sqlParserConfig);

        SqlNode exp = parser.parseStmtList();
        System.out.println(exp);
    }

    @Test
    public void connectionTest() throws Exception {
        Connection connection = connect();

        connection.setAutoCommit(false);
        Statement s = connection.createStatement();

        s.execute("create schema s");
        s.execute("create table s.books (id int, title varchar(50), author varchar(50), price float, qty int)");

        s.execute("insert into s.books values (1001, 'Java for dummies', 'Tan Ah Teck', 11.11, 11)");
        s.execute("insert into s.books values (1002, 'More Java for dummies', 'Tan Ah Teck', 22.22, 22)");
        s.execute("insert into s.books values (1003, 'More Java for more dummies', 'Mohammad Ali', 33.33, 33)");
        s.execute("insert into s.books values (1004, 'A Cup of Java', 'Kumar', 44.44, 44)");
        s.execute("insert into s.books values (1005, 'A Teaspoon of Java', 'Kevin Jones', 55.55, 55)");

        try (ResultSet r = s.executeQuery("select id, title from s.books")) { 
            assertThat(r.next(), is(true));
            assertThat(r.getInt(1), is(1001));
            assertThat(r.next(), is(true));
        }
         

    }

    @Test
    void testCreateSchema() throws Exception {
        try (Connection c = connect(); Statement s = c.createStatement()) {
            boolean b = s.execute("create schema s");
            assertThat(b, is(false));
            b = s.execute("create table s.t (i int not null)");
            assertThat(b, is(false));
            int x = s.executeUpdate("insert into s.t values 1");
            assertThat(x, is(1));
            try (ResultSet r = s.executeQuery("select count(*) from s.t")) {
                assertThat(r.next(), is(true));
                assertThat(r.getInt(1), is(1));
                assertThat(r.next(), is(false));
            }
        }
    }

    private Connection connect() throws SQLException {
        Properties config = new Properties();

        config.put(CalciteConnectionProperty.PARSER_FACTORY.camelName(), ServerDdlExecutor.class.getName() + "#PARSER_FACTORY");
//        config.put(CalciteConnectionProperty.MATERIALIZATIONS_ENABLED.camelName(), "true");
        config.put(CalciteConnectionProperty.FUN.camelName(), "standard,oracle");
//        config.put(CalciteConnectionProperty.MODEL.camelName(), "target/test-classes/calcite/model.json");

        Connection connection = DriverManager.getConnection("jdbc:calcite:", config);
        return connection;
    }
}
