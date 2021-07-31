/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.choam.fsm;

import com.chiralbehaviors.tron.Entry; 

/**
 * Reconfigures the view for the committee
 * 
 * @author hal.hildebrand
 *
 */
public enum Regenerate implements Regeneration.Transitions {
    BUILD {

        @Override
        public Regeneration.Transitions assumeDelegate() {
            return DELEGATE;
        }

        @Override
        public Regeneration.Transitions assumePrincipal() {
            return PRINCIPAL;
        }

        @Entry
        public void establishPrincipal() {
            context().establishPrincipal();
        }
    },
    DELEGATE {
        @Entry
        public void awaitView() {
            context().awaitView();
        }
    },
    ESTABLISHED, GENERATED {
 

        @Entry
        public void establishPrincipal() {
            context().establishPrincipal();
        }
    },
    PRINCIPAL {
        @Entry
        public void generateView() {
            context().generateView();
        }
    };
  
    @Override
    public Regeneration.Transitions establish() {
        return ESTABLISHED;
    }
 
    @Override
    public Regeneration.Transitions generated() {
        return GENERATED;
    }
}
