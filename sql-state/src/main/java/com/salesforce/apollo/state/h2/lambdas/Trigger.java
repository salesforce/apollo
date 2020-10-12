/*
 * Copyright (c) 2020, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.state.h2.lambdas;

import java.sql.Connection;
import java.sql.SQLException;

/**
 * @author hal.hildebrand
 *
 */
public class Trigger implements org.h2.api.Trigger {
    
   

    @Override
    public void init(Connection conn, String schemaName, String triggerName, String tableName, boolean before,
                     int type) throws SQLException {
        // TODO Auto-generated method stub
        
    }

    @Override
    public void fire(Connection conn, Object[] oldRow, Object[] newRow) throws SQLException {
        // TODO Auto-generated method stub
        
    }

    @Override
    public void close() throws SQLException {
        // TODO Auto-generated method stub
        
    }

    @Override
    public void remove() throws SQLException {
        // TODO Auto-generated method stub
        
    }

}
