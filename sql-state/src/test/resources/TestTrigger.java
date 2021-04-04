import java.sql.Connection;
import java.sql.SQLException;

import org.h2.api.Trigger;

/**
 * @author hal.hildebrand
 *
 */
public class TestTrigger implements Trigger {

    @Override
    public void init(Connection conn, String schemaName, String triggerName, String tableName, boolean before,
                     int type) throws SQLException {
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
