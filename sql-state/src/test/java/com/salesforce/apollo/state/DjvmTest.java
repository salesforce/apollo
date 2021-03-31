/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.state;

import java.io.File;
import java.nio.file.Path;
import java.util.Collections;
import java.util.function.Function;

import org.junit.jupiter.api.Test;

import com.salesforce.apollo.protocols.Utils;
import com.salesforce.apollo.state.functions.Functions;

import net.corda.djvm.SandboxConfiguration;
import net.corda.djvm.SandboxRuntimeContext;
import net.corda.djvm.TypedTaskFactory;
import net.corda.djvm.analysis.AnalysisConfiguration;
import net.corda.djvm.execution.ExecutionProfile;
import net.corda.djvm.messages.Severity;
import net.corda.djvm.rewiring.SandboxClassLoader;
import net.corda.djvm.source.BootstrapClassLoader;

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
        Functions funcs = new Functions();
        Class<?> clazz = funcs.compile("SimpleTask",
                                       Utils.getDocument(getClass().getResourceAsStream("/SimpleTask.java")), null);

        Path deterministicRt = new File("target/deterministic-rt.jar").toPath();
        BootstrapClassLoader bootstrap = new BootstrapClassLoader(deterministicRt);
        AnalysisConfiguration analysis = AnalysisConfiguration.createRoot(funcs, Collections.emptySet(), Severity.TRACE,
                                                                          bootstrap);
        ExecutionProfile execution = ExecutionProfile.DEFAULT;
        SandboxConfiguration config = SandboxConfiguration.createFor(analysis, execution);
        config.preload();
        SandboxRuntimeContext ctx = new SandboxRuntimeContext(config);
        example(ctx, clazz);
    }

    void example(SandboxRuntimeContext context, Class clazz) {
        context.use(ctx -> {
            SandboxClassLoader cl = ctx.getClassLoader();

            // Create a reusable task factory.
            TypedTaskFactory taskFactory;
            try {
                taskFactory = cl.createTypedTaskFactory();
            } catch (ClassNotFoundException | NoSuchMethodException e) {
                throw new IllegalStateException();
            }

            // Wrap SimpleTask inside an instance of sandbox.Task.
            @SuppressWarnings("unchecked")
            Function<long[], Long> simpleTask = taskFactory.create(clazz);

            // Execute SimpleTask inside the sandbox.
            Long result = simpleTask.apply(new long[] { 1000, 200, 30, 4 });
        });
    }
}
