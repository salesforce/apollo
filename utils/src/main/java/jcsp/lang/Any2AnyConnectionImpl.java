
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
 * This class is an implementation of <code>Any2AnyConnection</code>. Each end
 * is safe to be used by one thread at a time.
 *
 * @author Quickstone Technologies Limited
 */
class Any2AnyConnectionImpl extends AbstractConnectionImpl implements Any2AnyConnection {
    private One2OneChannel chanToServer;
    private One2OneChannel chanFromServer;
    private Any2OneChannel chanClientSynch;
    private Any2OneChannel chanServerSynch;

    /**
     * Initializes all the attributes to necessary values. Channels are created
     * using the static factory in the <code>ChannelServer</code> inteface.
     *
     * Constructor for One2OneConnectionImpl.
     */
    public Any2AnyConnectionImpl() {
        super();
        chanToServer = ConnectionServer.FACTORY.createOne2One(new Buffer(1));
        chanFromServer = ConnectionServer.FACTORY.createOne2One(new Buffer(1));
        chanClientSynch = ConnectionServer.FACTORY.createAny2One(new Buffer(1));
        chanServerSynch = ConnectionServer.FACTORY.createAny2One(new Buffer(1));
    }

    /**
     * Returns a <code>SharedAltingConnectionClient</code> object for this
     * connection. This method can be called multiple times to return a new
     * <code>SharedAltingConnectionClient</code> object each time. Any object
     * created can only be used by one process at a time but the set of objects
     * constructed can be used concurrently.
     *
     * @return a new <code>SharedAltingConnectionClient</code> object.
     */
    @Override
    public SharedAltingConnectionClient client() {
        return new SharedAltingConnectionClient(chanFromServer.in(), chanClientSynch.in(), chanToServer.out(),
                                                chanToServer.out(), chanClientSynch.out(), chanFromServer.out(), this);
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
        return new SharedConnectionServerImpl(chanToServer.in(), chanToServer.in(), chanServerSynch.in(),
                                              chanServerSynch.out(), this);
    }
}
