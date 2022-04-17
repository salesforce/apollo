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
public enum SimpleServer implements SimpleFsm {
    ACCEPTED, AWAIT_MESSAGE, PROCESS_MESSAGE, SESSION_ESTABLISHED,;

    @Override
    public SimpleFsm accepted(BufferHandler buffer) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public SimpleFsm closing() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public SimpleFsm connected(BufferHandler buffer) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public SimpleFsm protocolError() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public SimpleFsm readError() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public SimpleFsm readReady() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public SimpleFsm sendGoodbye() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public SimpleFsm transmitMessage(String message) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public SimpleFsm writeError() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public SimpleFsm writeReady() {
        // TODO Auto-generated method stub
        return null;
    }
}
