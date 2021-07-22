
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

import jcsp.util.Buffer;

/**
 * This class is an implementation of <code>One2AnyConnection</code>. Each end
 * is safe to be used by one thread at a time.
 *
 * @author Quickstone Technologies Limited
 */
class One2AnyConnectionImpl implements One2AnyConnection {
    private AltingConnectionClient client;
    private One2OneChannel         chanToServer;
    private One2OneChannel         chanFromServer;
    private Any2OneChannel         chanSynch;

    /**
     * Initializes all the attributes to necessary values. Channels are created
     * using the static factory in the <code>ChannelServer</code> interface.
     *
     * Constructor for One2OneConnectionImpl.
     */
    public One2AnyConnectionImpl() {
        super();
        chanToServer = ConnectionServer.FACTORY.createOne2One(new Buffer(1));
        chanFromServer = ConnectionServer.FACTORY.createOne2One(new Buffer(1));
        chanSynch = ConnectionServer.FACTORY.createAny2One(new Buffer(1));

        // create the client and server objects
        client = new AltingConnectionClientImpl(chanFromServer.in(), chanToServer.out(), chanToServer.out(),
                                                chanFromServer.out());
    }

    /**
     * Returns the <code>AltingConnectionClient</code> that can be used by a single
     * process at any instance.
     *
     * Each call to this method will return the same object reference.
     *
     * @return the <code>AltingConnectionClient</code> object.
     */
    @Override
    public AltingConnectionClient client() {
        return client;
    }

    /**
     * Returns a <code>SharedConnectionServer</code> object for this connection.
     * This method can be called multiple times to return a new
     * <code>SharedConnectionServer</code> object each time. Any object created can
     * only be used by one process at a time but the set of objects constructed can
     * be used concurrently.
     *
     * @return a new <code>SharedConnectionServer</code> object.
     */
    @Override
    public SharedConnectionServer server() {
        return new SharedConnectionServerImpl(chanToServer.in(), chanToServer.in(), chanSynch.in(), chanSynch.out(),
                                              this);
    }
}
