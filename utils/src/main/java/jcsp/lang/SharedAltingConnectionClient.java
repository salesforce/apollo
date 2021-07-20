
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
 * Implements a client end of a Connection which can have multiple client
 * processes.
 * </p>
 * <p>
 * This object cannot itself be shared between concurrent processes but
 * duplicate objects can be generated that can be used by multiple concurrent
 * processes. This can be achieved using the <code>{@link #duplicate()}</code>
 * method.
 * </p>
 * <p>
 * The reply from the server can be ALTed over.
 * </p>
 *
 * @author Quickstone Technologies Limited
 */
public class SharedAltingConnectionClient extends AltingConnectionClientImpl implements SharedConnectionClient {
    private ChannelInput                     synchIn;
    private ChannelOutput                    synchOut;
    private ConnectionWithSharedAltingClient parent;

    protected SharedAltingConnectionClient(AltingChannelInput fromServer, ChannelInput synchIn,
                                           ChannelOutput openToServer, ChannelOutput reqToServer,
                                           SharedChannelOutput synchOut, ChannelOutput backToClient,
                                           ConnectionWithSharedAltingClient parent) {
        super(fromServer, openToServer, reqToServer, backToClient);
        this.synchIn = synchIn;
        this.synchOut = synchOut;
        this.parent = parent;
    }

    @Override
    protected final void claim() {
        synchOut.write(null);
    }

    @Override
    protected final void release() {
        synchIn.read();
    }

    /**
     * <p>
     * Returns a <code>SharedConnectionClient</code> object that is a duplicate of
     * the object on which this method is called.
     * </p>
     * <p>
     * This allows a process using a <code>SharedAltingConnectionClient</code>
     * object to pass references to the connection client to multiple processes.
     * </p>
     * <p>
     * The object returned can be cast into a <code>SharedConnectionClient</code>
     * object.
     * </p>
     *
     * @return a duplicate <code>SharedAltingConnectionClient</code> object.
     */
    @Override
    public SharedConnectionClient duplicate() {
        return parent.client();
    }
}
