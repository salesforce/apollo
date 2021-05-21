/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.consortium;

import com.salesforce.apollo.comm.Router;

/**
 * @author hal.hildebrand
 *
 */
public class Genesis {
    public static class Parameters {
        public final Router communications;

        public Parameters(Router communications) {
            this.communications = communications;
        }

        public class Builder {
            public Router communications;

            public Router getCommunications() {
                return communications;
            }

            public void setCommunications(Router communications) {
                this.communications = communications;
            }

            public Parameters build() {
                return new Parameters(communications);
            }
        }
    }

    public Genesis(Parameters parameters) {
        
    }
}
