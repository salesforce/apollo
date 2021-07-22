
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
 * An interface to connection. This is used by servers which wish to
 * {@link Alternative ALT} over a connection. Note that you cannot have more
 * than one server serving an AltingConnectionServer.
 *
 * @see ConnectionServer
 * @see ConnectionClient
 * @see Connection
 *
 * @author Quickstone Technologies Limited
 */
public abstract class AltingConnectionServer extends Guard implements ConnectionServer {
    /**
     * The channel used to ALT over.
     */
    private AltingChannelInput altingChannel;

    /**
     * Constructor.
     *
     * Note that this is only intended for use by JCSP, and should not be called by
     * user processes. Users should use one of the subclasses.
     *
     * @param altingChannel The channel used to implement the Guard
     */
    protected AltingConnectionServer(AltingChannelInput altingChannel) {
        this.altingChannel = altingChannel;
    }

    /**
     * Returns the channel used to implement the Guard.
     *
     * Note that this method is only intended for use by JCSP, and should not be
     * called by user processes.
     *
     * Concrete subclasses should override this method to return null, to ensure
     * that the alting channel is kept private.
     *
     * @return The channel passed to the constructor.
     */
    protected AltingChannelInput getAltingChannel() {
        return altingChannel;
    }

    /**
     * <code>ConnectionServer</code> implementations are likely to be implemented
     * over channels. Multiple channels from the client to server may be used; one
     * could be used for the initial connection while another one could be used for
     * data requests.
     *
     * This method allows sub-classes to specify which channel should be the next
     * one to be alted over.
     *
     * @param chan the channel to be ALTed over.
     */
    protected void setAltingChannel(AltingChannelInput chan) {
        altingChannel = chan;
    }

    /**
     * Returns true if the event is ready. Otherwise, this enables the guard for
     * selection and returns false.
     * <P>
     * <I>Note: this method should only be called by the Alternative class</I>
     *
     * @param alt the Alternative class that is controlling the selection
     * @return true if and only if the event is ready
     */
    @Override
    boolean enable(Alternative alt) {
        return altingChannel.enable(alt);
    }

    /**
     * Disables the guard for selection. Returns true if the event was ready.
     * <P>
     * <I>Note: this method should only be called by the Alternative class</I>
     *
     * @return true if and only if the event was ready
     */
    @Override
    boolean disable() {
        return altingChannel.disable();
    }

    /**
     * Returns whether there is an open() pending on this connection.
     * <p>
     *
     * <i>Note: if there is, it won't go away until you accept it. But if there
     * isn't, there may be one by the time you check the result of this method.</i>
     *
     * @return true only if open() will complete without blocking.
     */
    public boolean pending() {
        return altingChannel.pending();
    }
}
