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

import com.chiralbehaviors.tron.Entry;
import com.chiralbehaviors.tron.Exit;
import com.chiralbehaviors.tron.Fsm;

/**
 * 
 * @author hhildebrand
 * 
 */
public enum PhoneNumber implements TelephoneFsm {
    DialTone() {
        /**
         * If an invalid digit is dialed, give up collecting digits immediately.
         */
        @Override
        public TelephoneFsm digit(String digit) {
            int d = Integer.parseInt(digit);
            if (d < 0 || d > 9) {
                fsm().pop().invalidDigit();
                context().clearDisplay();
                return null;
            } else if (d == 1) {
                Telephone context = context();
                context.playTT(d);
                context.setType(Telephone.CallType.LONG_DISTANCE);
                context.saveAreaCode(d);
                context.addDisplay("-");
                return LongDistance;
            } else if (d == 9) {
                Telephone context = context();
                context.playTT(d);
                context.saveExchange(d);
                return OneOneStart;
            } else {
                Telephone context = context();
                context.playTT(d);
                context.setType(Telephone.CallType.LOCAL);
                context.saveExchange(d);
                return Exchange;
            }
        }

        @Entry
        public void entry() {
            Telephone context = context();
            context.loop("dialtone");
            context.startTimer("OffHookTimer", 10000);
        }

        @Exit
        public void exit() {
            Telephone context = context();
            context.stopTimer("OffHookTimer");
            context.stopLoop("dialtone");
        }
    },
    Exchange() {
        @Override
        public TelephoneFsm digit(String digit) {
            int d = Integer.parseInt(digit);
            Telephone context = context();
            if (d < 0 || d > 9) {
                fsm().pop().invalidDigit();
                context().clearDisplay();
                return null;
            } else if (context.getExchange().length() < 2) {
                context.playTT(d);
                context.saveExchange(d);
                context.resetTimer("OffHookTimer");
                return null;
            } else {
                context.playTT(d);
                context.saveExchange(d);
                context.addDisplay("-");
                return LocalCall;
            }
        }

        @Entry
        public void entry() {
            context().startTimer("OffHookTimer", 10000);
        }

        @Exit
        public void exit() {
            context().stopTimer("OffHookTimer");
        }
    },
    LocalCall() {
        @Override
        public TelephoneFsm digit(String digit) {
            int d = Integer.parseInt(digit);
            Telephone context = context();
            if (d < 0 || d > 9) {
                fsm().pop().invalidDigit();
                context().clearDisplay();
                return null;
            } else if (context.getLocal().length() < 3) {
                context.playTT(d);
                context.saveLocal(d);
                context.resetTimer("OffHookTimer");
                return null;
            } else {
                fsm().pop()
                     .dialingDone(context.getType(), context.getAreaCode(), context.getExchange(), context.getLocal());
                context.playTT(d);
                context.saveLocal(d);
                return null;
            }
        }

        @Entry
        public void entry() {
            context().startTimer("OffHookTimer", 10000);
        }

        @Exit
        public void exit() {
            context().stopTimer("OffHookTimer");
        }
    },
    LongDistance() {
        @Override
        public TelephoneFsm digit(String digit) {
            int d = Integer.parseInt(digit);
            Telephone context = context();
            if (d < 0 || d > 9) {
                fsm().pop().invalidDigit();
                context().clearDisplay();
                return null;
            } else if (context.getAreaCode().length() < 3) {
                context.playTT(d);
                context.saveAreaCode(d);
                context.resetTimer("OffHookTimer");
                return null;
            } else {
                context.playTT(d);
                context.saveAreaCode(d);
                context.addDisplay("-");
                return Exchange;
            }
        }

        @Entry
        public void entry() {
            context().startTimer("OffHookTimer", 10000);
        }

        @Exit
        public void exit() {
            context().stopTimer("OffHookTimer");
        }
    },
    NineOne() {
        @Override
        public TelephoneFsm digit(String digit) {
            int d = Integer.parseInt(digit);
            Telephone context = context();
            if (d < 0 || d > 9) {
                fsm().pop().invalidDigit();
                context().clearDisplay();
                return null;
            } else if (d == 1) {
                fsm().pop()
                     .dialingDone(context.getType(), context.getAreaCode(), context.getExchange(), context.getLocal());
                context.playTT(d);
                context.setType(Telephone.CallType.EMERGENCY);
                context.saveExchange(d);
                return null;
            } else {
                context.playTT(d);
                context.setType(Telephone.CallType.LOCAL);
                context.saveExchange(d);
                context.addDisplay("-");
                return LocalCall;
            }
        }

        @Entry
        public void entry() {
            context().startTimer("OffHookTimer", 10000);
        }

        @Exit
        public void exit() {
            context().stopTimer("OffHookTimer");
        }
    },
    OneOneStart() {
        @Override
        public TelephoneFsm digit(String digit) {
            int d = Integer.parseInt(digit);
            Telephone context = context();
            if (d < 0 || d > 9) {
                fsm().pop().invalidDigit();
                context().clearDisplay();
                return null;
            } else if (d == 1) {
                context.playTT(d);
                context.saveExchange(d);
                return NineOne;
            } else {
                context.playTT(d);
                context.setType(Telephone.CallType.LOCAL);
                context.saveExchange(d);
                return Exchange;
            }
        }

        @Entry
        public void entry() {
            context().startTimer("OffHookTimer", 10000);
        }

        @Exit
        public void exit() {
            context().stopTimer("OffHookTimer");
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
        throw fsm().invalidTransitionOn();
    }

    @Override
    public TelephoneFsm digit(String digit) {
        throw fsm().invalidTransitionOn();
    }

    @Override
    public TelephoneFsm emergency() {
        throw fsm().invalidTransitionOn();
    }

    @Override
    public TelephoneFsm invalidDigit() {
        fsm().pop().invalidDigit();
        context().clearDisplay();
        return null;
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
        fsm().pop().leftOfHook();
        context().clearDisplay();
        return null;
    }

    @Override
    public TelephoneFsm onHook() {
        fsm().pop().onHook();
        context().clearDisplay();
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
