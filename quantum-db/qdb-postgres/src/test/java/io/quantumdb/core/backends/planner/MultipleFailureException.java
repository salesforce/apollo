/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package io.quantumdb.core.backends.planner;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * @author hal.hildebrand
 *
 */
public class MultipleFailureException extends Exception {
    private static final long serialVersionUID = 1L;

    /**
     * Asserts that a list of throwables is empty. If it isn't empty, will throw
     * {@link MultipleFailureException} (if there are multiple throwables in the
     * list) or the first element in the list (if there is only one element).
     *
     * @param errors list to check
     * @throws Throwable if the list is not empty
     */
    public static void assertEmpty(List<Throwable> errors) throws Throwable {
        if (errors.isEmpty()) {
            return;
        }
        if (errors.size() == 1) {
            throw errors.get(0);
        }

        /*
         * Many places in the code are documented to throw
         * org.junit.internal.runners.model.MultipleFailureException. That class now
         * extends this one, so we throw the internal exception in case developers have
         * tests that catch MultipleFailureException.
         */
        throw new MultipleFailureException(errors);
    }

    private final List<Throwable> fErrors;

    public MultipleFailureException(List<Throwable> errors) {
        fErrors = new ArrayList<Throwable>(errors);
    }

    public List<Throwable> getFailures() {
        return Collections.unmodifiableList(fErrors);
    }

    @Override
    public String getMessage() {
        StringBuilder sb = new StringBuilder(String.format("There were %d errors:", fErrors.size()));
        for (Throwable e : fErrors) {
            sb.append(String.format("\n  %s(%s)", e.getClass().getName(), e.getMessage()));
        }
        return sb.toString();
    }
}
