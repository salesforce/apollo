/*
 * Copyright (c) 2019, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo;

import static org.junit.Assert.assertEquals;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.function.Consumer;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * A smoke test for the shaded Apollo jar, with example configuration
 * 
 * @author hal.hildebrand
 * @since 220
 */
public class E2ETest {

	static {
		ApolloConfiguration.SimCommunicationsFactory.reset();
	}
	
    class StreamGobbler implements Runnable {
        private InputStream inputStream;
        private Consumer<String> consumeInputLine;

        public StreamGobbler(InputStream inputStream, Consumer<String> consumeInputLine) {
            this.inputStream = inputStream;
            this.consumeInputLine = consumeInputLine;
        }

        @Override
        public void run() {
            new BufferedReader(new InputStreamReader(inputStream)).lines().forEach(consumeInputLine);
        }
    }

    private Process process;

    @After
    public void after() {
        System.out.println("shutting down server");
        if (process != null) {
            process.destroyForcibly();
        }
    }

    @Before
    public void before() throws Exception {
        String java = System.getProperty("java.home") + "/bin/java";
        ProcessBuilder builder = new ProcessBuilder();
        builder.command(java, "--illegal-access=permit", "-jar", "target/apollo-web.jar", "server", "target/test-classes/server.yml");
        process = builder.start();
        StreamGobbler outputGobbler = new StreamGobbler(process.getInputStream(), string -> {
            try {
                System.out.println(string);
            } catch (Throwable e) {
                // ignore
            }
        });
        StreamGobbler errorGobbler = new StreamGobbler(process.getErrorStream(), string -> {
            try {
                System.out.println(string);
            } catch (Throwable e) {
                // ignore
            }
        });

        Thread thread = new Thread(outputGobbler);
        thread.setDaemon(true);
        thread.start();
        thread = new Thread(errorGobbler);
        thread.setDaemon(true);
        thread.start(); 
    }

    @Test
    public void e2eTest() throws Exception {
        
        Thread.sleep(5_000);
        process.destroyForcibly();
        
        assertEquals(137, process.waitFor());
    }
}
