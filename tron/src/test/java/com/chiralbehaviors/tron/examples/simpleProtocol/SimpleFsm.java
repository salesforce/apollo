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
package com.chiralbehaviors.tron.examples.simpleProtocol;

import com.chiralbehaviors.tron.FsmExecutor;
import com.chiralbehaviors.tron.InvalidTransition;

/**
 * 
 * @author hhildebrand
 * 
 */
public interface SimpleFsm extends FsmExecutor<SimpleProtocol, SimpleFsm> {
    default SimpleFsm accepted(BufferHandler buffer) {
        return protocolError();
    }

    default SimpleFsm closing() {
        return protocolError();
    }

    default SimpleFsm connected(BufferHandler buffer) {
        return protocolError();
    }

    default SimpleFsm protocolError() {
        throw new InvalidTransition();
    }

    default SimpleFsm readError() {
        return protocolError();
    }

    default SimpleFsm readReady() {
        return protocolError();
    }

    default SimpleFsm sendGoodbye() {
        return protocolError();
    }

    default SimpleFsm transmitMessage(String message) {
        return protocolError();
    }

    default SimpleFsm writeError() {
        return protocolError();
    }

    default SimpleFsm writeReady() {
        return protocolError();
    }
}
