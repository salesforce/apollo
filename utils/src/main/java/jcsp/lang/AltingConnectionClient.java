
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
 * <p>
 * This class is sub-classed by JCSP.NET classes to provide
 * <code>ConnectionClient</code> objects which can have their
 * <code>receive()</code> method alted over.
 * </p>
 * <p>
 * Although JCSP users could sub-class this class, under most circumstances,
 * there is no need. <code>AltingConnectionClient</code> objects can be
 * constructed using one of the Connection factory mechanisms. See
 * <code>{@link Connection}</code> and
 * <code>{@link StandardConnectionFactory}</code>.
 * </p>
 *
 * @author Quickstone Technologies Limited
 */
public abstract class AltingConnectionClient<In, Out> extends Guard implements ConnectionClient<In, Out> {
    /**
     * The channel used to ALT over.
     */
    private AltingChannelInput<In> altingChannel;

    /**
     * <p>
     * Constructor.
     * </p>
     * <p>
     * Note that this is only intended for use by JCSP, and should not be called by
     * user processes. Users should use one of the subclasses.
     * </p>
     * 
     * @param altingChannel The channel used to implement the Guard
     */
    protected AltingConnectionClient(AltingChannelInput<In> altingChannel) {
        this.altingChannel = altingChannel;
    }

    /**
     * <p>
     * Returns the channel used to implement the Guard.
     * </p>
     * <p>
     * Note that this method is only intended for use by JCSP, and should not be
     * called by user processes.
     * </p>
     * <p>
     * Concrete subclasses should override this method to return null, to ensure
     * that the alting channel is kept private.
     * </p>
     * 
     * @return The channel passed to the constructor.
     */
    protected AltingChannelInput<In> getAltingChannel() {
        return this.altingChannel;
    }

    /**
     * <p>
     * <code>ConnectionServer</code> implementations are likely to be implemented
     * over channels. Multiple channels from the client to server may be used; one
     * could be used for the initial connection while another one could be used for
     * data requests.
     * </p>
     * <p>
     * This method allows sub-classes to specify which channel should be the next
     * one to be alted over.
     * </p>
     *
     * @param chan the channel to be ALTed over.
     */
    protected void setAltingChannel(AltingChannelInput<In> chan) {
        this.altingChannel = chan;
    }

    /**
     * <p>
     * Returns true if the event is ready. Otherwise, this enables the guard for
     * selection and returns false.
     * </p>
     * <p>
     * <I>Note: this method should only be called by the Alternative class</I>
     * </p>
     * 
     * @param alt the Alternative class that is controlling the selection
     * @return true if and only if the event is ready
     */
    @Override
    boolean enable(Alternative alt) {
        return altingChannel.enable(alt);
    }

    /**
     * <p>
     * Disables the guard for selection. Returns true if the event was ready.
     * </p>
     * <p>
     * <I>Note: this method should only be called by the Alternative class</I>
     * </p>
     * 
     * @return true if and only if the event was ready
     */
    @Override
    boolean disable() {
        return altingChannel.disable();
    }

    /**
     * <p>
     * Returns whether there is an open() pending on this connection.
     * <p>
     * </p>
     * <p>
     * <i>Note: if there is, it won't go away until you accept it. But if there
     * isn't, there may be one by the time you check the result of this method.</i>
     * </p>
     * 
     * @return true only if open() will complete without blocking.
     */
    public boolean pending() {
        return altingChannel.pending();
    }
}
