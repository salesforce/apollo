package com.salesforce.apollo.state;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;
import java.util.zip.DeflaterOutputStream;

import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.parser.SqlParser.Config;
import org.apache.calcite.sql.parser.ddl.SqlDdlParserImpl;
import org.apache.calcite.sql.validate.SqlConformanceEnum;
import org.h2.jdbc.JdbcConnection;
import org.junit.jupiter.api.Test;

import com.google.common.io.CharStreams;
import com.salesforce.apollo.protocols.Conversion;
import com.salesforce.apollo.protocols.HashKey;

public class SmokeTest {

    @Test
    public void checkpoint() throws Exception {
        JdbcConnection db1 = new JdbcConnection("jdbc:h2:mem:chkpt1", new Properties());
        JdbcConnection db2 = new JdbcConnection("jdbc:h2:mem:chkpt2", new Properties());
        mutate(db1).close();
        db1.commit();
        mutate(db2).close();
        db2.commit();

        File chkpnt1File = new File("target/chkpnt1.sql");
        File chkpnt2File = new File("target/chkpnt2.sql");
        chkpnt1File.delete();
        chkpnt2File.delete();

        Statement s1 = db1.createStatement();
        s1.execute("script to 'target/chkpnt1.sql'");
        Statement s2 = db2.createStatement();
        s2.execute("script to 'target/chkpnt2.sql'");

        assertEquals(chkpnt1File.length(), chkpnt2File.length());

        File chkpnt1zip = new File("target/chkpnt1.sql.zip");
        File chkpnt2zip = new File("target/chkpnt2.sql.zip");
        chkpnt1zip.delete();
        chkpnt2zip.delete();

        compress(chkpnt1File, chkpnt1zip);
        compress(chkpnt2File, chkpnt2zip);

        assertEquals(chkpnt1zip.length(), chkpnt2zip.length());

        HashKey hash1 = new HashKey(Conversion.hashOf(new FileInputStream(chkpnt1zip)));
        HashKey hash2 = new HashKey(Conversion.hashOf(new FileInputStream(chkpnt2zip)));

        assertEquals(hash1, hash2);
    } 

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

    private Statement mutate(Connection connection) throws SQLException {
        connection.setAutoCommit(false);
        Statement s = connection.createStatement();

        s.execute("create schema s");
        s.execute("create table s.books (id int, title varchar(50), author varchar(50), price float, qty int)");

        s.execute("insert into s.books values (1001, 'Java for dummies', 'Tan Ah Teck', 11.11, 11)");
        s.execute("insert into s.books values (1002, 'More Java for dummies', 'Tan Ah Teck', 22.22, 22)");
        s.execute("insert into s.books values (1003, 'More Java for more dummies', 'Mohammad Ali', 33.33, 33)");
        s.execute("insert into s.books values (1004, 'A Cup of Java', 'Kumar', 44.44, 44)");
        s.execute("insert into s.books values (1005, 'A Teaspoon of Java', 'Kevin Jones', 55.55, 55)");
        return s;
    }

    private void compress(File input, File output) throws Exception {
        try (FileInputStream fis = new FileInputStream(input); FileOutputStream fos = new FileOutputStream(output)) {
            DeflaterOutputStream dos = new DeflaterOutputStream(fos);
            int data;
            while ((data = fis.read()) != -1) {
                dos.write(data);
            }
            dos.flush();
            dos.close();
        }
    }
}
