/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.state;

import java.io.File;
import java.util.function.Function;

import org.junit.jupiter.api.Test;

import com.salesforce.apollo.protocols.Utils;
import com.salesforce.apollo.state.functions.Functions;

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
        try (Functions funcs = new Functions(Functions.defaultConfig(), ExecutionProfile.DEFAULT,
                dir)) {
            Class<?> clazz = funcs.compile("SimpleTask",
                                           Utils.getDocument(getClass().getResourceAsStream("/SimpleTask.java")));
          funcs.execute(clazz);
        }
    }
}
