/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package io.quantumdb.core.backends.planner;

import static org.hamcrest.MatcherAssert.assertThat;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;

import org.hamcrest.Matcher;

/**
 * @author hal.hildebrand
 *
 */
public class ErrorCollector {
    private List<Throwable> errors = new ArrayList<Throwable>();

    /**
     * Adds a Throwable to the table. Execution continues, but the test will fail at
     * the end.
     */
    public void addError(Throwable error) {
        errors.add(error);
    }

    /**
     * Adds to the table the exception, if any, thrown from {@code callable}.
     * Execution continues, but the test will fail at the end if {@code callable}
     * threw an exception.
     */
    public Object checkSucceeds(Callable<Object> callable) {
        try {
            return callable.call();
        } catch (Throwable e) {
            addError(e);
            return null;
        }
    }

    /**
     * Adds a failure with the given {@code reason} to the table if {@code matcher}
     * does not match {@code value}. Execution continues, but the test will fail at
     * the end if the match fails.
     */
    public <T> void checkThat(final String reason, final T value, final Matcher<T> matcher) {
        checkSucceeds(new Callable<Object>() {
            @Override
            public Object call() throws Exception {
                assertThat(reason, value, matcher);
                return value;
            }
        });
    }

    /**
     * Adds a failure to the table if {@code matcher} does not match {@code value}.
     * Execution continues, but the test will fail at the end if the match fails.
     */
    public <T> void checkThat(final T value, final Matcher<T> matcher) {
        checkThat("", value, matcher);
    }

    public void verify() throws Throwable {
        MultipleFailureException.assertEmpty(errors);
    }
}
