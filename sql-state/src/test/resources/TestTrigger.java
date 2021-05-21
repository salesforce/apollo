import java.sql.*;
import java.util.Arrays;
import java.util.List;

import org.h2.api.Trigger;

import sandbox.java.lang.DJVM;

/**
 * @author hal.hildebrand
 *
 */
public class TestTrigger implements Trigger {

    @Override
    public void init(Connection conn, String schemaName, String triggerName, String tableName, boolean before,
                     int type) throws SQLException {
    }

    @Override
    public void fire(Connection conn, Object[] oldRow, Object[] newRow) throws SQLException {
        conn.setAutoCommit(false);
        Statement s = conn.createStatement();
        List<String> ids = Arrays.asList("1002", "1004");
        for (int i = 0; i < ids.size(); i++) {
            String id = ids.get(i);
            s.execute("update s.books set qty = " + 666 + " where id = " + id);
        }
    }

    @Override
    public void close() throws SQLException {
        throw new SQLException();
    }

    @Override
    public void remove() throws SQLException {
        throw new SQLException();
    }
}
