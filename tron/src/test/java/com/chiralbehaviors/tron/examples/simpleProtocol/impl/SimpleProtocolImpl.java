package com.chiralbehaviors.tron.examples.simpleProtocol.impl;

import com.chiralbehaviors.tron.examples.simpleProtocol.BufferHandler;
import com.chiralbehaviors.tron.examples.simpleProtocol.SimpleProtocol;

public class SimpleProtocolImpl implements SimpleProtocol {
    private BufferHandler handler;

    @Override
    public void ackReceived() {
        // TODO Auto-generated method stub

    }

    @Override
    public void awaitAck() {
        // TODO Auto-generated method stub

    }

    @Override
    public void enableSend() {
        // TODO Auto-generated method stub

    }

    @Override
    public void establishClientSession() {
        // TODO Auto-generated method stub

    }

    public BufferHandler getHandler() {
        return handler;
    }

    @Override
    public void sendGoodbye() {
        // TODO Auto-generated method stub

    }

    @Override
    public void setHandler(BufferHandler handler) {
        this.handler = handler;
    }

    @Override
    public void transmitMessage(String message) {
        // TODO Auto-generated method stub

    }

}
