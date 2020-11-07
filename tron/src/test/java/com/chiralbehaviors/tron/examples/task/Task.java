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

package com.chiralbehaviors.tron.examples.task;

import com.chiralbehaviors.tron.Exit;

/**
 * 
 * @author hhildebrand
 * 
 */
public enum Task implements TaskFsm {
    /**
     * the uncompleted task is externally prevented from running again. It will stay
     * in this state until either stopped or unblocked.
     */
    Blocked() {
        /**
         * The task may continue working now. No actions needed.
         */
        @Override
        public TaskFsm unblock() {
            return Suspended;
        }

    },
    /**
     * the task is completely stopped and all associated resources returned. The
     * task may now be safely deleted. This is the FSM end state.
     */
    Deleted() {

        @Override
        public TaskFsm delete() {
            return null;
        }

        @Override
        public TaskFsm stop() {
            return null;
        }
    },
    /**
     * the task is actively doing work. The task is allowed to run for a specified
     * time limit.
     */
    Running() {
        @Override
        public TaskFsm block() {
            TaskModel context = context();
            context.blockTask();
            return Blocked;
        }

        @Override
        public TaskFsm done() {
            TaskModel context = context();
            context.releaseResources();
            return Stopped;
        }

        @Exit
        public void exit() {
            context().stopSliceTimer();
        }

        /**
         * Wait for another time slice.
         */
        @Override
        public TaskFsm suspended() {
            TaskModel context = context();
            context.suspendTask();
            return Suspended;
        }
    },
    /**
     * the task has either completed running or externally stopped.
     */
    Stopped() {

        @Override
        public TaskFsm stop() {
            return null;
        }

    },
    /**
     * the task is cleaning up allocated resources before entering the stop state.
     */
    Stopping {

        @Override
        public TaskFsm stop() {
            return null;
        }

        @Override
        public TaskFsm stopped() {
            context().releaseResources();
            return Stopped;
        }

    },
    /**
     * the task is waiting to run again since it is not yet completed.
     */
    Suspended() {
        @Override
        public TaskFsm block() {
            context().blockTask();
            return Blocked;
        }

        /**
         * Time to do more work. The timeslice duration is passed in as a transition
         * argument.
         */
        @Override
        public TaskFsm start(long timeslice) {
            TaskModel context = context();
            context.continueTask();
            context.startSliceTimer(timeslice);
            return Running;
        }

    };

    /**
     * All but the Delete state follow this transition. Define it here.
     */
    @Override
    public TaskFsm delete() {
        return Deleted;
    }

    /**
     * Three states follow this transition, three states ignore. So define the
     * active definition.
     */
    @Override
    public TaskFsm stop() {
        context().stopTask();
        return Stopping;
    }
}
