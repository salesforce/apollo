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

/**
 * 
 * @author hhildebrand
 * 
 */
public interface Telephone {
    public enum CallType {
        EMERGENCY, LOCAL, LONG_DISTANCE;
    }

    String LONG_DISTANCE = null;

    void addDisplay(String string);

    void clearDisplay();

    String getAreaCode();

    String getExchange();

    String getLocal();

    int getType();

    void loop(String string);

    void playDepositMoney();

    void playEmergency();

    void playInvalidNumber();

    void playNYCTemp();

    void playTime();

    void playTT(int d);

    void resetTimer(String string);

    void routeCall(int callType, String areaCode, String exchange, String local);

    void saveAreaCode(int d);

    void saveExchange(int d);

    void saveLocal(int d);

    void setReceiver(String string, String string2);

    void setType(CallType longDistance);

    void startClockTimer();

    void startTimer(String string, int i);

    void stopLoop(String string);

    void stopPlayback();

    void stopTimer(String timer);

    void updateClock();
}
