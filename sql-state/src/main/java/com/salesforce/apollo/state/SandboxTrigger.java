/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.state;

import java.lang.reflect.Method;
import java.sql.Connection;
import java.sql.SQLException;

import org.h2.api.Trigger;

import net.corda.djvm.SandboxRuntimeContext;
import net.corda.djvm.rewiring.SandboxClassLoader;
import sandbox.java.lang.DJVM;

/**
 * @author hal.hildebrand
 *
 */
public class SandboxTrigger implements Trigger {

    private Method                      close;
    private final SandboxRuntimeContext context;
    private Method                      fire;
    private Method                      init;
    private Method                      remove;
    private final Object                trigger;

    public SandboxTrigger(SandboxRuntimeContext context, Object object) throws Exception {
        this.context = context;
        this.trigger = object;
        Class<? extends Object> triggerClass = object.getClass();
        for (Method m : triggerClass.getDeclaredMethods()) {
            switch (m.getName()) {
            case "close": {
                close = m;
            }
            case "init": {
                init = m;
            }
            case "fire": {
                fire = m;
            }
            case "remove": {
                remove = m;
            }
            default:
            }
        }
    }

    @Override
    public void close() throws SQLException {
        context.use(ctx -> {
            try {
                close.invoke(trigger);
            } catch (Throwable e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
        });
    }

    @Override
    public void fire(Connection conn, Object[] oldRow, Object[] newRow) throws SQLException {
        context.use(ctx -> {
            SandboxClassLoader cl = ctx.getClassLoader();
            try { 
                Class<?> wrapperClass = cl.loadClass("sandbox.com.salesforce.apollo.dsql.ConnectionWrapper");
                Object wrappedConnection = wrapperClass.getDeclaredConstructor(java.sql.Connection.class)
                                                       .newInstance(new Object[] {conn});
                System.out.println(wrappedConnection);
                DJVM.sandbox(newRow);
                fire.invoke(trigger, wrappedConnection, null, null);
            } catch (Throwable e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
        });
    }

    @Override
    public void init(Connection conn, String schemaName, String triggerName, String tableName, boolean before,
                     int type) throws SQLException {
        context.use(ctx -> {
            try {
                init.invoke(trigger, conn, schemaName, triggerName, tableName, before);
            } catch (Throwable e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
        });
    }

    @Override
    public void remove() throws SQLException {
        context.use(ctx -> {
            try {
                remove.invoke(trigger);
            } catch (Throwable e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
        });
    }

}
