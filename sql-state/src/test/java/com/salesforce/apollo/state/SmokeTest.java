/*
 * Copyright (c) 2020, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.state;

import java.sql.Connection;
import java.sql.Statement;
import java.util.Properties;

import org.junit.jupiter.api.Test;

import com.salesforce.apollo.state.h2.CdcEngine;

/**
 * @author hal.hildebrand
 *
 */
public class SmokeTest {

    @Test
    public void smoke() throws Exception {
        CdcEngine engine = new CdcEngine("jdbc:h2:mem:test", new Properties());
        Connection conn = engine.getConnection(); 
        
        Statement stmt = conn.createStatement();
        stmt.execute("create table emp (id integer primary key, name text)");
        conn.commit();
        
        stmt.execute("insert into emp values(1, 'foo')");
        conn.commit();
        
        conn.close();
    }
}
