package com.chiralbehaviors.tron.examples.simpleProtocol.impl;

import com.chiralbehaviors.tron.examples.simpleProtocol.BufferHandler;
import com.chiralbehaviors.tron.examples.simpleProtocol.SimpleProtocol;

public class SimpleProtocolImpl implements SimpleProtocol {
    private BufferHandler handler;

    @Override
    public void ackReceived() {
    }

    @Override
    public void awaitAck() {
    }

    @Override
    public void enableSend() {
    }

    @Override
    public void establishClientSession() {
    }

    public BufferHandler getHandler() {
        return handler;
    }

    @Override
    public void sendGoodbye() {
    }

    @Override
    public void setHandler(BufferHandler handler) {
        this.handler = handler;
    }

    @Override
    public void transmitMessage(String message) {
    }

}
