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
package com.chiralbehaviors.tron.examples.telephone;

import com.chiralbehaviors.tron.Default;
import com.chiralbehaviors.tron.Entry;
import com.chiralbehaviors.tron.Exit;
import com.chiralbehaviors.tron.Fsm;

/**
 * 
 * @author hhildebrand
 * 
 */
public enum Call implements TelephoneFsm {
    BusySignal() {
        @Entry
        public void entry() {
            context().loop("busy");
        }

        @Exit
        public void exit() {
            context().stopLoop("busy");
        }
    },
    DepositMoney() {
        @Entry
        public void entry() {
            Telephone context = context();
            context.loop("ringing");
            context.startTimer("RingTimer", 5000);
        }

        @Exit
        public void exit() {
            Telephone context = context();
            context.stopTimer("RingTimer");
            context.stopLoop("ringing");
        }

        @Override
        public TelephoneFsm ringTimer() {
            context().playDepositMoney();
            return PlayingMessage;
        }
    },
    /**
     * The number is being dialed.
     */
    Dialing() {

        @Override
        public TelephoneFsm dialingDone(int callType, String areaCode, String exchange, String local) {
            context().routeCall(callType, areaCode, exchange, local);
            return Routing;
        }

        @Override
        public TelephoneFsm invalidDigit() {
            return InvalidDigit;
        }

        @Override
        public TelephoneFsm leftOfHook() {
            return LeftOffHook;
        }

    },
    InvalidDigit() {
        @Default
        public TelephoneFsm defaultTransition() {
            return null;
        }

        @Entry
        public void entry() {
            Telephone context = context();
            context.startTimer("LoopTimer", 10000);
            context.loop("fast_busy");
        }

        @Exit
        public void exit() {
            Telephone context = context();
            context.stopTimer("LoopTimer");
            context.stopLoop("fast_busy");
        }

        @Override
        public TelephoneFsm loopTimer() {
            return WaitForOnHook;
        }
    },
    LeftOffHook() {
        @Default
        public TelephoneFsm defaultTransition() {
            return null;
        }

        @Entry
        public void entry() {
            Telephone context = context();
            context.startTimer("LoopTimer", 10000);
            context.loop("phone_off_hook");
        }

        @Exit
        public void exit() {
            Telephone context = context();
            context.stopTimer("LoopTimer");
            context.stopLoop("phone_off_hook");
        }

        @Override
        public TelephoneFsm loopTimer() {
            return WaitForOnHook;
        }
    },
    MessagePlayed() {
        @Entry
        public void entry() {
            context().startTimer("OffHookTimer", 10000);
        }

        @Exit
        public void exit() {
            context().stopTimer("OffHookTimer");
        }

        @Override
        public TelephoneFsm offHookTimer() {
            return LeftOffHook;
        }
    },
    NYCTemp() {
        @Entry
        public void entry() {
            Telephone context = context();
            context.loop("ringing");
            context.startTimer("RingTimer", 10000);
        }

        @Exit
        public void exit() {
            Telephone context = context();
            context.stopTimer("RingTimer");
            context.stopLoop("ringing");
        }

        @Override
        public TelephoneFsm ringTimer() {
            context().playNYCTemp();
            return PlayingMessage;
        }
    },
    OnHook() {
        /**
         * Time to update the clock's display.
         */
        @Override
        public TelephoneFsm clockTimer() {
            Telephone context = context();
            context.updateClock();
            context.startClockTimer();
            return null;
        }

        @Entry
        public void entry() {
            Telephone context = context();
            context.updateClock();
            context.startClockTimer();
        }

        @Exit
        public void exit() {
            context().stopTimer("ClockTimer");
        }

        /**
         * We are handling the caller's side of the connection.
         */
        @Override
        public TelephoneFsm offHook() {
            fsm().push(PhoneNumber.DialTone);
            Telephone context = context();
            context.clearDisplay();
            context.setReceiver("on hook", "Put down receiver");
            return Dialing;
        }

        /**
         * Oops.
         */
        @Override
        public TelephoneFsm onHook() {
            return null;
        }
    },
    PlayingMessage() {
        @Override
        public TelephoneFsm onHook() {
            Telephone context = context();
            context.stopPlayback();
            context.setReceiver("off hook", "Pick up receiver");
            context.clearDisplay();
            return OnHook;
        }
    },
    /**
     * The call is now being routed.
     */
    Routing() {
        @Override
        public TelephoneFsm depositMoney() {
            return DepositMoney;
        }

        @Override
        public TelephoneFsm emergency() {
            context().playEmergency();
            return PlayingMessage;
        }

        @Override
        public TelephoneFsm invalidNumber() {
            context().playInvalidNumber();
            return PlayingMessage;
        }

        @Override
        public TelephoneFsm lineBusy() {
            return BusySignal;
        }

        @Override
        public TelephoneFsm nycTemp() {
            return NYCTemp;
        }

        @Override
        public TelephoneFsm time() {
            return Time;
        }
    },
    Time() {
        @Entry
        public void entry() {
            Telephone context = context();
            context.loop("ringing");
            context.startTimer("RingTimer", 10000);
        }

        @Exit
        public void exit() {
            Telephone context = context();
            context.stopTimer("RingTimer");
            context.stopLoop("ringing");
        }

        @Override
        public TelephoneFsm ringTimer() {
            context().playTime();
            return PlayingMessage;
        }
    },
    WaitForOnHook() {
        @Default
        public TelephoneFsm defaultTransition() {
            return null;
        }
    };

    private static Telephone context() {
        Telephone context = Fsm.thisContext();
        return context;
    }

    private static Fsm<Telephone, TelephoneFsm> fsm() {
        Fsm<Telephone, TelephoneFsm> fsm = Fsm.thisFsm();
        return fsm;
    }

    @Override
    public TelephoneFsm clockTimer() {
        return null;
    }

    @Override
    public TelephoneFsm depositMoney() {
        throw fsm().invalidTransitionOn();
    }

    @Override
    public TelephoneFsm dialingDone(int callType, String areaCode, String exchange, String local) {
        throw null;
    }

    @Override
    public TelephoneFsm digit(String digit) {
        return null;
    }

    @Override
    public TelephoneFsm emergency() {
        throw fsm().invalidTransitionOn();
    }

    @Override
    public TelephoneFsm invalidDigit() {
        throw null;
    }

    @Override
    public TelephoneFsm invalidNumber() {
        throw fsm().invalidTransitionOn();
    }

    @Override
    public TelephoneFsm leftOfHook() {
        throw fsm().invalidTransitionOn();
    }

    @Override
    public TelephoneFsm lineBusy() {
        throw fsm().invalidTransitionOn();
    }

    @Override
    public TelephoneFsm loopTimer() {
        throw fsm().invalidTransitionOn();
    }

    @Override
    public TelephoneFsm nycTemp() {
        throw fsm().invalidTransitionOn();
    }

    @Override
    public TelephoneFsm offHook() {
        throw fsm().invalidTransitionOn();
    }

    @Override
    public TelephoneFsm offHookTimer() {
        throw fsm().invalidTransitionOn();
    }

    /**
     * No matter when it happens, when the phone is hung up, this call is OVER!
     */
    @Override
    public TelephoneFsm onHook() {
        Telephone context = context();
        context.setReceiver("off hook", "Pick up receiver");
        context.clearDisplay();
        return null;
    }

    @Override
    public TelephoneFsm ringTimer() {
        throw fsm().invalidTransitionOn();
    }

    @Override
    public TelephoneFsm time() {
        throw fsm().invalidTransitionOn();
    }
}
