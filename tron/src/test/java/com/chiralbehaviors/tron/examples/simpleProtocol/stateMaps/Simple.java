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
package com.chiralbehaviors.tron.examples.simpleProtocol.stateMaps;

import com.chiralbehaviors.tron.examples.simpleProtocol.BufferHandler;
import com.chiralbehaviors.tron.examples.simpleProtocol.SimpleFsm;

/**
 * 
 * @author hhildebrand
 * 
 */
public enum Simple implements SimpleFsm {
    CLOSED, CONNECTED() {
        @Override
        public SimpleFsm closing() {
            return CLOSED;
        }

        @Override
        public SimpleFsm readError() {
            return CLOSED;
        }

        @Override
        public SimpleFsm writeError() {
            return CLOSED;
        }
    },
    INITIAL() {
        @Override
        public SimpleFsm accepted(BufferHandler handler) {
            context().setHandler(handler);
            fsm().push(SimpleServer.ACCEPTED);
            return CONNECTED;
        }

        @Override
        public SimpleFsm connected(BufferHandler handler) {
            context().setHandler(handler);
            fsm().push(SimpleClient.CONNECTED);
            return CONNECTED;
        }
    },
    PROTOCOL_ERROR() {

    };

    @Override
    public SimpleFsm accepted(BufferHandler buffer) {
        return PROTOCOL_ERROR;
    }

    @Override
    public SimpleFsm closing() {
        return CLOSED;
    }

    @Override
    public SimpleFsm connected(BufferHandler buffer) {
        return PROTOCOL_ERROR;
    }

    @Override
    public SimpleFsm protocolError() {
        return PROTOCOL_ERROR;
    }

    @Override
    public SimpleFsm readError() {
        return CLOSED;
    }

    @Override
    public SimpleFsm readReady() {
        return CLOSED;
    }

    @Override
    public SimpleFsm sendGoodbye() {
        return PROTOCOL_ERROR;
    }

    @Override
    public SimpleFsm transmitMessage(String message) {
        return PROTOCOL_ERROR;
    }

    @Override
    public SimpleFsm writeError() {
        return CLOSED;
    }

    @Override
    public SimpleFsm writeReady() {
        return PROTOCOL_ERROR;
    }
}
