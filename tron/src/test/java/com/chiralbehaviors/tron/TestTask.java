/*
 * Copyright (c) ChiralBehaviors LLC, all rights reserved.
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
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import org.junit.jupiter.api.Test;
import org.mockito.internal.verification.Times;

import com.chiralbehaviors.tron.examples.task.Task;
import com.chiralbehaviors.tron.examples.task.TaskFsm;
import com.chiralbehaviors.tron.examples.task.TaskModel;

/**
 * 
 * @author hhildebrand
 * 
 */
public class TestTask {
    @Test
    public void testIt() {
        long timeslice = 100;
        TaskModel model = mock(TaskModel.class);
        Fsm<TaskModel, TaskFsm> fsm = Fsm.construct(model, TaskFsm.class, Task.Suspended, false);
        TaskFsm transitions = fsm.getTransitions();
        assertEquals(Task.Suspended, fsm.getCurrentState());
        transitions.start(timeslice);
        verify(model).continueTask();
        verify(model).startSliceTimer(timeslice);
        assertEquals(Task.Running, fsm.getCurrentState());
        transitions.suspended();
        assertEquals(Task.Suspended, fsm.getCurrentState());
        verify(model).stopSliceTimer();
        transitions.start(timeslice);
        verify(model, new Times(2)).startSliceTimer(timeslice);
        transitions.block();
        assertEquals(Task.Blocked, fsm.getCurrentState());
        verify(model, new Times(2)).stopSliceTimer();
        transitions.unblock();
        assertEquals(Task.Suspended, fsm.getCurrentState());
        transitions.start(timeslice);
        verify(model, new Times(3)).startSliceTimer(timeslice);
        transitions.stop();
        assertEquals(Task.Stopping, fsm.getCurrentState());
        verify(model, new Times(3)).stopSliceTimer();
        transitions.stopped();
        assertEquals(Task.Stopped, fsm.getCurrentState());
        transitions.delete();
        assertEquals(Task.Deleted, fsm.getCurrentState());
        transitions.delete();
        assertEquals(Task.Deleted, fsm.getCurrentState());
        transitions.stop();
        assertEquals(Task.Deleted, fsm.getCurrentState());
    }
}
