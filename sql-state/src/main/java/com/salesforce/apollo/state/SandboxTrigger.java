/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.state;

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;
import java.sql.Connection;
import java.sql.SQLException;

import org.h2.api.Trigger;

import net.corda.djvm.SandboxRuntimeContext;

/**
 * @author hal.hildebrand
 *
 */
public class SandboxTrigger implements Trigger {

    private final MethodHandle          close;
    private final SandboxRuntimeContext context;
    private final MethodHandle          fire;
    private final MethodHandle          init;
    private final MethodHandle          remove;
    private final Object                wrapper;

    public SandboxTrigger(SandboxRuntimeContext context, Object wrapper) throws Exception {
        this.context = context;
        this.wrapper = wrapper;
        Class<? extends Object[]> objectArrayClass = new Object[0].getClass();
        Class<? extends Object> wrapperClass = wrapper.getClass();
        close = MethodHandles.lookup().findVirtual(wrapperClass, "close", MethodType.methodType(void.class));
        init = MethodHandles.lookup()
                            .findVirtual(wrapperClass, "init",
                                         MethodType.methodType(void.class, Connection.class, String.class, String.class,
                                                               String.class, boolean.class, int.class));
        fire = MethodHandles.lookup()
                            .findVirtual(wrapperClass, "fire",
                                         MethodType.methodType(void.class, Connection.class, objectArrayClass,
                                                               objectArrayClass));
        remove = MethodHandles.lookup().findVirtual(wrapperClass, "remove", MethodType.methodType(void.class));
    }

    @Override
    public void close() throws SQLException {
        context.use(ctx -> {
            try {
                close.invokeExact(wrapper);
            } catch (Throwable e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
        });
    }

    @Override
    public void fire(Connection conn, Object[] oldRow, Object[] newRow) throws SQLException {
        context.use(ctx -> {
            try {
                fire.invoke(wrapper, conn, oldRow, newRow);
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
                init.invokeExact(wrapper, conn, schemaName, triggerName, tableName, before);
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
                remove.invokeExact(wrapper);
            } catch (Throwable e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
        });
    }

}
