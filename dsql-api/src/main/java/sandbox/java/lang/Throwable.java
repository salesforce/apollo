/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package sandbox.java.lang;

/**
 * @author hal.hildebrand
 *
 */
@SuppressWarnings("serial")
public class Throwable extends java.lang.Exception {
    public Throwable(String message, Throwable t) {
        super();
    }

    public Throwable(String message) {
        super();
    }

    public Throwable(Throwable t) {
        super(t);
    }

    public Throwable() {
    }

    public Throwable getCause() {
        return null;
    }
}
