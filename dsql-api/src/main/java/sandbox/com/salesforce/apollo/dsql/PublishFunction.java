/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package sandbox.com.salesforce.apollo.dsql;

import sandbox.java.lang.String;
import sandbox.java.sql.Connection;
import sandbox.java.sql.PreparedStatement;
import sandbox.java.sql.SQLException;

/**
 * @author hal.hildebrand
 *
 */
public class PublishFunction {

    private static final String PUBLISH_INSERT = String.toDJVM("INSERT INTO __APOLLO_INTERNAL__.TRAMPOLINE(CHANNEL, BODY) VALUES(?1, ?2)");

    public static boolean publish(Connection connection, String channel, String jsonBody) {
        try (PreparedStatement statement = connection.prepareStatement(PUBLISH_INSERT)) {
            statement.setString(1, channel);
            statement.setString(2, jsonBody);
            statement.execute();
        } catch (SQLException e) {
            throw new IllegalStateException("Unable to publish: " + channel, e);
        }
        return true;
    }
}
