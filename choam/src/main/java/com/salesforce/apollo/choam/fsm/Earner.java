/*
 * Copyright (c) 2021, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.choam.fsm;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.chiralbehaviors.tron.Entry;
import com.salesforce.apollo.choam.fsm.Driven.Transitions; 

/**
 * Producer Finite State Machine
 * 
 * @author hal.hildebrand
 *
 */
public enum Earner implements Driven.Transitions {
    DELEGATE {

        @Entry
        public void startProduction() {
            context().startProduction();
        } 
    },
    INITIAL {
        @Override
        public Transitions regenerate() {
            return Reconfigure.GATHER;
        }

        @Override
        public Transitions start() {
            context().initialState();
            return null;
        }
    },
    PRINCIPAL {
        @Entry
        public void startProduction() {
            context().startProduction();
        } 
    },
    PROTOCOL_FAILURE {
        @Entry
        public void terminate() {
            log.error("Protocol failure", new Exception("Protocol failure at: " + fsm().getPreviousState()));
        }
    };
    private static final Logger log =  LoggerFactory.getLogger(Earner.class);

    @Override
    public Transitions assumeDelegate() {
        return DELEGATE;
    }

    @Override
    public Transitions assumePrincipal() {
        return PRINCIPAL;
    }
}
