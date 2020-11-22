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

/**
 * The exception indicating that the transition is invalid for the current state
 * of the Fsm
 * 
 * @author hhildebrand
 * 
 */
public class InvalidTransition extends RuntimeException {

    private static final long serialVersionUID = 1L;

    public InvalidTransition(String msg) {
        super(msg);
    }

    public InvalidTransition(String string, Throwable e) {
        super(string, e);
    }

    @SuppressWarnings("unused")
    private InvalidTransition() {
    }
}
