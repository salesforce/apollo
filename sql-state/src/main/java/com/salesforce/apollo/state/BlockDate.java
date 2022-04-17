/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.state;

import java.util.Date;

/**
 * @author hal.hildebrand
 *
 */
public class BlockDate extends Date {

    private static final long serialVersionUID = 1L;

    public BlockDate() {
        // TODO Auto-generated constructor stub
    }

    public BlockDate(long date) {
        super(date);
    }

    @SuppressWarnings("deprecation")
    public BlockDate(int year, int month, int date) {
        super(year, month, date);
    }

    @SuppressWarnings("deprecation")
    public BlockDate(int year, int month, int date, int hrs, int min) {
        super(year, month, date, hrs, min);
    }

    @SuppressWarnings("deprecation")
    public BlockDate(int year, int month, int date, int hrs, int min, int sec) {
        super(year, month, date, hrs, min, sec);
    }

}
