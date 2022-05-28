package com.salesforce.apollo.state;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.Random;
import java.util.zip.DeflaterOutputStream;

import deterministic.org.h2.jdbc.JdbcConnection;
import org.junit.jupiter.api.Test;

import com.salesforce.apollo.crypto.Digest;
import com.salesforce.apollo.crypto.DigestAlgorithm;

public class SmokeTest {

    private static final Random entropy = new Random();

    @Test
    public void checkpoint() throws Exception {
        File chkpnt1File = new File("target/chkpnt1.sql");
        File chkpnt2File = new File("target/chkpnt2.sql");
        JdbcConnection db1 = new JdbcConnection("jdbc:h2:mem:chkpt1", new Properties(), null, null, false);
        JdbcConnection db2 = new JdbcConnection("jdbc:h2:mem:chkpt2", new Properties(), null, null, false);

        createAndInsert(db1).close();
        db1.commit();

        createAndInsert(db2).close();
        db2.commit();

        chkpnt1File.delete();
        chkpnt2File.delete();

        Statement s1 = db1.createStatement();
        s1.execute("script to 'target/chkpnt1.sql'");
        Statement s2 = db2.createStatement();
        s2.execute("script to 'target/chkpnt2.sql'");

        List<String> odds = Arrays.asList("1001", "1003", "1005");
        List<Integer> oddValues = Arrays.asList(entropy.nextInt(), entropy.nextInt(), entropy.nextInt());

        update(db1, odds, oddValues).close();
        db1.commit();

        update(db2, odds, oddValues).close();
        db2.commit();

        List<String> evens = Arrays.asList("1002", "1004");
        List<Integer> evenValues = Arrays.asList(entropy.nextInt(), entropy.nextInt());

        update(db1, evens, evenValues).close();
        db1.commit();

        update(db2, evens, evenValues).close();
        db2.commit();

        chkpnt1File.delete();
        chkpnt2File.delete();

        s1 = db1.createStatement();
        s1.execute("script to 'target/chkpnt1.sql'");
        s2 = db2.createStatement();
        s2.execute("script to 'target/chkpnt2.sql'");

        assertEquals(chkpnt1File.length(), chkpnt2File.length());

        File chkpnt1zip = new File("target/chkpnt1.sql.zip");
        File chkpnt2zip = new File("target/chkpnt2.sql.zip");
        chkpnt1zip.delete();
        chkpnt2zip.delete();

        compress(chkpnt1File, chkpnt1zip);
        compress(chkpnt2File, chkpnt2zip);

        assertEquals(chkpnt1zip.length(), chkpnt2zip.length());

        Digest hash1 = DigestAlgorithm.DEFAULT.digest(new FileInputStream(chkpnt1zip));
        Digest hash2 = DigestAlgorithm.DEFAULT.digest(new FileInputStream(chkpnt2zip));

        assertEquals(hash1, hash2);
    }

    private Statement createAndInsert(Connection connection) throws SQLException {
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

    private Statement update(Connection connection, List<String> ids, List<Integer> values) throws SQLException {
        connection.setAutoCommit(false);
        Statement s = connection.createStatement();
        for (int i = 0; i < ids.size(); i++) {
            String id = ids.get(i);
            int value = values.get(i);
            try {
                s.execute("update s.books set qty = " + value + " where id = " + id);
            } catch (SQLException e) {
                fail("update", e);
            }
        }
        ;
        return s;
    }

    private void compress(File input, File output) throws Exception {
        try (FileInputStream fis = new FileInputStream(input); FileOutputStream fos = new FileOutputStream(output)) {
            DeflaterOutputStream dos = new DeflaterOutputStream(fos);
            int data;
            while ((data = fis.read()) != -1) {
                dos.write(data);
            }
            dos.finish();
            dos.flush();
            dos.close();
        }
    }
}
