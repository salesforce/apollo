/*
 * Copyright (c) 2022, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */

package com.salesforce.apollo.thoth;

import java.io.ByteArrayOutputStream;
import java.io.OutputStream;

import org.slf4j.Logger;

/**
 * @author hal.hildebrand
 *
 */
public class LoggingOutputStream extends OutputStream {

    public enum LogLevel {
        DEBUG, ERROR, INFO, TRACE, WARN,
    }

    private final ByteArrayOutputStream baos = new ByteArrayOutputStream(1000);
    private final LogLevel              level;

    private final Logger logger;

    public LoggingOutputStream(Logger logger, LogLevel level) {
        this.logger = logger;
        this.level = level;
    }

    @Override
    public void write(int b) {
        if (b == '\n') {
            String line = baos.toString();
            baos.reset();

            switch (level) {
            case TRACE:
                logger.trace(line);
                break;
            case DEBUG:
                logger.debug(line);
                break;
            case ERROR:
                logger.error(line);
                break;
            case INFO:
                logger.info(line);
                break;
            case WARN:
                logger.warn(line);
                break;
            }
        } else {
            baos.write(b);
        }
    }

}
