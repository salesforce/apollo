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

/**
 * 
 * @author hhildebrand
 * 
 */
public interface SimpleFsm extends FsmExecutor<SimpleProtocol, SimpleFsm> {
    SimpleFsm accepted(BufferHandler buffer);

    SimpleFsm closing();

    SimpleFsm connected(BufferHandler buffer);

    SimpleFsm protocolError();

    SimpleFsm readError();

    SimpleFsm readReady();

    SimpleFsm sendGoodbye();

    SimpleFsm transmitMessage(String message);

    SimpleFsm writeError();

    SimpleFsm writeReady();
}
