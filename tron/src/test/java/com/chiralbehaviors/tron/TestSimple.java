/*
 * Copyright (c) 2013 ChiralBehaviors LLC, all rights reserved.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.chiralbehaviors.tron;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import org.junit.jupiter.api.Test;

import com.chiralbehaviors.tron.examples.simpleProtocol.BufferHandler;
import com.chiralbehaviors.tron.examples.simpleProtocol.SimpleFsm;
import com.chiralbehaviors.tron.examples.simpleProtocol.SimpleProtocol;
import com.chiralbehaviors.tron.examples.simpleProtocol.impl.SimpleProtocolImpl;
import com.chiralbehaviors.tron.examples.simpleProtocol.stateMaps.Simple;
import com.chiralbehaviors.tron.examples.simpleProtocol.stateMaps.SimpleClient;

/**
 * 
 * @author hhildebrand
 * 
 */
public class TestSimple {
    @Test
    public void testIt() {
        SimpleProtocol protocol = new SimpleProtocolImpl();
        Fsm<SimpleProtocol, SimpleFsm> fsm = Fsm.construct(protocol, SimpleFsm.class, Simple.INITIAL, true);
        verifyFsmStates(fsm, protocol);
    }

    @Test
    public void testItWithCustomClassLoader() {
        SimpleProtocol protocol = new SimpleProtocolImpl();
        Fsm<SimpleProtocol, SimpleFsm> fsm = Fsm.construct(protocol, SimpleFsm.class, SimpleFsm.class.getClassLoader(),
                                                           Simple.INITIAL, true);
        verifyFsmStates(fsm, protocol);
    }

    private void verifyFsmStates(Fsm<SimpleProtocol, SimpleFsm> fsm, SimpleProtocol protocol) {
        assertNotNull(fsm);
        BufferHandler handler = new BufferHandler();
        fsm.getTransitions().connected(handler);
        assertEquals(handler, ((SimpleProtocolImpl) protocol).getHandler());
        assertEquals(SimpleClient.CONNECTED, fsm.getCurrentState());
        fsm.getTransitions().writeReady();
        assertEquals(SimpleClient.ESTABLISH_SESSION, fsm.getCurrentState());
        fsm.getTransitions().readReady();
        assertEquals(SimpleClient.SEND_MESSAGE, fsm.getCurrentState());
        fsm.getTransitions().sendGoodbye();
        assertEquals(SimpleClient.SEND_GOODBYE, fsm.getCurrentState());
        fsm.getTransitions().readReady();
        assertEquals(Simple.CLOSED, fsm.getCurrentState());
    }
}
