
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
public class SharedConnectionServerImpl implements SharedConnectionServer {
    private AltingConnectionServerImpl connectionServerToUse;

    private ChannelInput                     synchIn;
    private ChannelOutput                    synchOut;
    private ConnectionWithSharedAltingServer parent;

    protected SharedConnectionServerImpl(AltingChannelInput openIn, AltingChannelInput requestIn, ChannelInput synchIn,
                                         SharedChannelOutput synchOut, ConnectionWithSharedAltingServer parent) {
        connectionServerToUse = new AltingConnectionServerImpl(openIn, requestIn);
        this.synchOut = synchOut;
        this.synchIn = synchIn;
        this.parent = parent;
    }

    @Override
    public Object request() {
        if (connectionServerToUse.getServerState() == AltingConnectionServerImpl.SERVER_STATE_CLOSED)
            synchOut.write(null);
        return connectionServerToUse.request();
    }

    @Override
    public void reply(Object data) {
        reply(data, false);
    }

    @Override
    public void reply(Object data, boolean close) {
        connectionServerToUse.reply(data, close);
        if (connectionServerToUse.getServerState() == AltingConnectionServerImpl.SERVER_STATE_CLOSED)
            synchIn.read();
    }

    @Override
    public void replyAndClose(Object data) {
        reply(data, true);
    }

    @Override
    public SharedConnectionServer duplicate() {
        return parent.server();
    }
}
