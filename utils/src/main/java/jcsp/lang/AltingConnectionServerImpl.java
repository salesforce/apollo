
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
public class AltingConnectionServerImpl extends AltingConnectionServer {

    /**
     * Server state. The server is initially <tt>CLOSED</tt> the first request will
     * take it to the <tt>RECEIVED</tt> state. A reply will take it back to
     * <tt>OPEN</tt> or <tt>CLOSED</tt> depending on the mode of reply. From the
     * <tt>OPEN</tt> or <tt>CLOSED</tt> state a further request can occur.
     */
    protected static final int SERVER_STATE_CLOSED = 1;

    /**
     * Server state. The server is initially <tt>CLOSED</tt> the first request will
     * take it to the <tt>RECEIVED</tt> state. A reply will take it back to
     * <tt>OPEN</tt> or <tt>CLOSED</tt> depending on the mode of reply. From the
     * <tt>OPEN</tt> or <tt>CLOSED</tt> state a further request can occur.
     */
    protected static final int SERVER_STATE_OPEN = 2;

    /**
     * Server state. The server is initially <tt>CLOSED</tt> the first request will
     * take it to the <tt>RECEIVED</tt> state. A reply will take it back to
     * <tt>OPEN</tt> or <tt>CLOSED</tt> depending on the mode of reply. From the
     * <tt>OPEN</tt> or <tt>CLOSED</tt> state a further request can occur.
     */
    protected static final int SERVER_STATE_RECEIVED = 3;

    private int currentServerState;

    private AltingChannelInput openIn;

    private AltingChannelInput furtherRequestIn;

    private ChannelInput currentInputChannel;

    private ChannelOutput toClient = null;

    private ConnectionServerMessage msg = null;

    /**
     * Constructs a new server instance. This must be called by a subclass which is
     * responsible for creating the channels.
     */
    protected AltingConnectionServerImpl(AltingChannelInput openIn, AltingChannelInput furtherRequestIn) {
        super(openIn);
        this.openIn = openIn;
        this.furtherRequestIn = furtherRequestIn;
        this.currentInputChannel = openIn;
        currentServerState = SERVER_STATE_CLOSED;
    }

    /**
     * Receives some data from a client once a connection has been established. This
     * will block until the client calls <code>request(Object)</code> but by
     * establishing a connection.
     *
     * @return the <code>Object</code> sent by the client.
     */
    @Override
    public Object request() throws IllegalStateException {
        if (currentServerState == SERVER_STATE_RECEIVED)
            throw new IllegalStateException("Cannot call request() twice on ConnectionServer without replying to the client first.");
        ConnectionClientMessage msg = (ConnectionClientMessage) currentInputChannel.read();

        if (currentServerState == SERVER_STATE_CLOSED) {
            if (msg instanceof ConnectionClientOpenMessage) {
                // channel to use to reply to client
                toClient = ((ConnectionClientOpenMessage) msg).replyChannel;
                setAltingChannel(furtherRequestIn);
                currentInputChannel = furtherRequestIn;

                // create a new msg for connection established
                // don't know if client implementation will have finished with
                // message after connection closed
                this.msg = new ConnectionServerMessage();
            } else
                throw new IllegalStateException("Invalid message received from client");
        }
        currentServerState = SERVER_STATE_RECEIVED;
        return msg.data;
    }

    /**
     * Sends some data back to the client after a request has been received but
     * keeps the connection open. After calling this method, the server should call
     * <code>recieve()</code> to receive a further request.
     *
     * @param data the data to send to the client.
     */
    @Override
    public void reply(Object data) throws IllegalStateException {
        reply(data, false);
    }

    /**
     * Sends some data back to the client after a request has been received. The
     * closed parameter indicates whether or not the connection should be closed.
     * The connection will be closed iff close is <code>true</code>.
     *
     * @param data  the data to send to the client.
     * @param close <code>boolean</code> indicating whether or not the connection
     *              should be closed.
     */
    @Override
    public void reply(Object data, boolean close) throws IllegalStateException {
        if (currentServerState != SERVER_STATE_RECEIVED)
            throw new IllegalStateException("Cannot call reply(Object, boolean) on a ConnectionServer that has not received an unacknowledge request.");

        // set open to true before replying
        msg.data = data;
        msg.open = !close;
        toClient.write(msg);
        if (close) {
            currentServerState = SERVER_STATE_CLOSED;
            toClient = null;
            setAltingChannel(openIn);
            currentInputChannel = openIn;
        } else
            currentServerState = SERVER_STATE_OPEN;
    }

    /**
     * Sends some data back to the client and closes the connection. This method
     * will not block. After calling this method, the server may call
     * <code>accept()</code> in order to allow another connection to this server to
     * be established.
     *
     * If this method did not take any data to send back to the client, and the
     * server was meant to call <code>reply(Object)</code> followed by a
     * <code>close()</code>, then there would be a race hazard at the client as it
     * would not know whether the connection had remained open or not.
     *
     * @param data the data to send back to client.
     */
    @Override
    public void replyAndClose(Object data) throws IllegalStateException {
        reply(data, true);
    }

    protected int getServerState() {
        return currentServerState;
    }
}
