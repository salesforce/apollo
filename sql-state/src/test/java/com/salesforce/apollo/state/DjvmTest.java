/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.state;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.File;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Properties;
import java.util.function.Function;

import org.h2.api.Trigger;
import org.h2.jdbc.JdbcConnection;
import org.junit.jupiter.api.Test;

import com.salesforce.apollo.protocols.Utils;

import net.corda.djvm.TypedTaskFactory;
import net.corda.djvm.execution.ExecutionProfile;

/**
 * @author hal.hildebrand
 *
 */
public class DjvmTest {

    public interface WithJava {

        static <T, R> R run(TypedTaskFactory taskFactory, Class<? extends Function<T, R>> taskClass, T input) {
            try {
                return taskFactory.create(taskClass).apply(input);
            } catch (Exception e) {
                throw asRuntime(e);
            }
        }

        static RuntimeException asRuntime(Throwable t) {
            return (t instanceof RuntimeException) ? (RuntimeException) t : new RuntimeException(t.getMessage(), t);
        }
    }

    @Test
    public void smoke() throws Exception {
        File dir = File.createTempFile("foo", "bar");
        dir.delete();
        dir.deleteOnExit();
        try (DeterministicCompiler funcs = new DeterministicCompiler(DeterministicCompiler.DEFAULT_CONFIG,
                ExecutionProfile.DEFAULT, dir)) {
            @SuppressWarnings("unchecked")
            Class<? extends Function<long[], Long>> clazz = (Class<? extends Function<long[], Long>>) funcs.compile("SimpleTask",
                                                                                                                    Utils.getDocument(getClass().getResourceAsStream("/SimpleTask.java")));
            funcs.execute(clazz);
            funcs.compileClass("TestTrigger", Utils.getDocument(getClass().getResourceAsStream("/TestTrigger.java")));
            Trigger trigger = funcs.loadTrigger("TestTrigger");
            assertNotNull(trigger);

            try (java.sql.Connection db1 = new JdbcConnection("jdbc:h2:mem:djvm", new Properties())) {
                db1.setAutoCommit(false);
                Statement s = db1.createStatement();

                s.execute("create schema s");
                s.execute("create table s.books (id int, title varchar(50), author varchar(50), price float, qty int)");

                s.execute("insert into s.books values (1001, 'Java for dummies', 'Tan Ah Teck', 11.11, 11)");
                s.execute("insert into s.books values (1002, 'More Java for dummies', 'Tan Ah Teck', 22.22, 22)");
                s.execute("insert into s.books values (1003, 'More Java for more dummies', 'Mohammad Ali', 33.33, 33)");
                s.execute("insert into s.books values (1004, 'A Cup of Java', 'Kumar', 44.44, 44)");
                s.execute("insert into s.books values (1005, 'A Teaspoon of Java', 'Kevin Jones', 55.55, 55)");
                db1.commit();
                s.close();
                trigger.fire(db1, new Object[] { "foo" }, new Object[] { "bar" });
                s = db1.createStatement();
                ResultSet result = s.executeQuery("select b.qty from s.books b where b.id = 1004");
                assertTrue(result.next());
                assertEquals(666, result.getInt(1));
            }
        }
    }
}
