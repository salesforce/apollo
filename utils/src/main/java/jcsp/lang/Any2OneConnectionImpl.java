
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
 * This class is an implementation of <code>Any2OneConnection</code>. Each end
 * is safe to be used by one thread at a time.
 *
 * @author Quickstone Technologies Limited
 */
class Any2OneConnectionImpl implements Any2OneConnection {
    private AltingConnectionServer server;
    private One2OneChannel         chanToServer;
    private One2OneChannel         chanFromServer;
    private Any2OneChannel         chanSynch;

    /**
     * Initializes all the attributes to necessary values. Channels are created
     * using the static factory in the <code>ChannelServer</code> inteface.
     *
     * Constructor for One2OneConnectionImpl.
     */
    public Any2OneConnectionImpl() {
        super();
        chanToServer = ConnectionServer.FACTORY.createOne2One(new Buffer(1));
        chanFromServer = ConnectionServer.FACTORY.createOne2One(new Buffer(1));
        chanSynch = ConnectionServer.FACTORY.createAny2One(new Buffer(1));
        // create the server object - client object created when accessed
        server = new AltingConnectionServerImpl(chanToServer.in(), chanToServer.in());
    }

    /**
     * Returns the <code>AltingConnectionClient</code> that can be used by a single
     * process at any instance.
     *
     * @return the <code>AltingConnectionClient</code> object.
     */
    @Override
    public SharedAltingConnectionClient client() {
        return new SharedAltingConnectionClient(chanFromServer.in(), chanSynch.in(), chanToServer.out(),
                                                chanToServer.out(), chanSynch.out(), chanFromServer.out(), this);
    }

    /**
     * Returns the <code>AltingConnectionServer</code> that can be used by a single
     * process at any instance.
     *
     * @return the <code>AltingConnectionServer</code> object.
     */
    @Override
    public AltingConnectionServer server() {
        return server;
    }
}
