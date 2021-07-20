
//////////////////////////////////////////////////////////////////////
//                                                                  //
//  JCSP ("CSP for Java") Libraries                                 //
//  Copyright (C) 1996-2018 Peter Welch, Paul Austin and Neil Brown //
//                2001-2004 Quickstone Technologies Limited         //
//                2005-2018 Kevin Chalmers                          //
//                                                                  //
//  You may use this work under the terms of either                 //
//  1. The Apache License, Version 2.0                              //
//  2. or (at your option), the GNU Lesser General Public License,  //
//       version 2.1 or greater.                                    //
//                                                                  //
//  Full licence texts are included in the LICENCE file with        //
//  this library.                                                   //
//                                                                  //
//  Author contacts: P.H.Welch@kent.ac.uk K.Chalmers@napier.ac.uk   //
//                                                                  //
//////////////////////////////////////////////////////////////////////

package jcsp.lang;

/**
 * This class does not need to be used by standard JCSP users. It is exposed so
 * that the connection mechanism can be extended for custom connections.
 *
 * @author Quickstone Technologies Limited
 */
public class AltingConnectionClientImpl extends AltingConnectionClient {
    private int currentClientState;

    private static final int CLIENT_STATE_CLOSED   = 1;
    private static final int CLIENT_STATE_MADE_REQ = 2;
    private static final int CLIENT_STATE_OPEN     = 3;

    private AltingChannelInput fromServer;

    private ChannelOutput openToServer;
    private ChannelOutput reqToServer;
    private ChannelOutput backToClient;

    private ConnectionClientMessage     msg     = new ConnectionClientMessage();
    private ConnectionClientOpenMessage msgOpen = new ConnectionClientOpenMessage();

    /**
     * Constructs a new instance. This constructor must be called by a subclass
     * which is responsible for creating the channels used by the connection and
     * must pass them into this constructor.
     */
    protected AltingConnectionClientImpl(AltingChannelInput fromServer, ChannelOutput openToServer,
                                         ChannelOutput reqToServer, ChannelOutput backToClient) {
        super(fromServer);
        this.fromServer = fromServer;
        this.openToServer = openToServer;
        this.reqToServer = reqToServer;
        this.backToClient = backToClient;
        currentClientState = CLIENT_STATE_CLOSED;
    }

    /**
     * Sends some data over the connection to server once the connection has been
     * opened.
     *
     * @param data the <code>Object</code> to send to the server.
     */
    @Override
    public void request(Object data) throws IllegalStateException {
        if (currentClientState == CLIENT_STATE_MADE_REQ)
            throw new IllegalStateException("Cannot call request(Object) twice without calling reply().");
        // this will claim the use of the client
        if (currentClientState == CLIENT_STATE_CLOSED) {
            claim();
            msgOpen.data = data;
            msgOpen.replyChannel = backToClient;
            openToServer.write(msgOpen);
        } else {
            msg.data = data;
            reqToServer.write(msg);
        }
        currentClientState = CLIENT_STATE_MADE_REQ;
    }

    /**
     * Receives some data back from the server after <code>request(Object)</code>
     * has been called.
     *
     * @return the <code>Object</code> sent from the server.
     */
    @Override
    public Object reply() throws IllegalStateException {
        if (currentClientState != CLIENT_STATE_MADE_REQ)
            throw new IllegalStateException("Cannot call reply() on a ConnectionClient that is not waiting for a reply.");
        ConnectionServerMessage serverReply = (ConnectionServerMessage) fromServer.read();

        // check whether the server closed the connection
        currentClientState = serverReply.open ? CLIENT_STATE_OPEN : CLIENT_STATE_CLOSED;
        if (serverReply.open)
            currentClientState = CLIENT_STATE_OPEN;
        else {
            currentClientState = CLIENT_STATE_CLOSED;
            release();
        }
        return serverReply.data;
    }

    /**
     * Returns whether the server has kept its end of the Connection open. This
     * should only be called after a call to <code>reply()</code> and before any
     * other Connection method is called.
     *
     * @return <code>true</code> iff the server has kept the connection open.
     */
    @Override
    public boolean isOpen() throws IllegalStateException {
        if (currentClientState == CLIENT_STATE_MADE_REQ)
            throw new IllegalStateException("Can only call isOpen() just after a reply has been received from the server.");
        return currentClientState == CLIENT_STATE_OPEN;
    }

    /**
     * This claims a lock on the client. This implementation does nothing as
     * instances of this class are only meant to be used with One2?Connection
     * objects.
     *
     */
    protected void claim() {
    }

    /**
     * This releases a lock on the client. This implementation does nothing as
     * instances of this class are only meant to be used with One2?Connection
     * objects.
     *
     */
    protected void release() {
    }
}
