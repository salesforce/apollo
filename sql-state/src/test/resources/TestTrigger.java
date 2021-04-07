import java.sql.*;

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
        
        PreparedStatement s = conn.prepareStatement("select * from foo");
        s.execute();
        throw new SQLException(conn.toString());
    }

    @Override
    public void fire(Connection conn, Object[] oldRow, Object[] newRow) throws SQLException {
        throw new SQLException(conn.toString());
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
