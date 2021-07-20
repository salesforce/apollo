
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
 * Defines an interface for a connection shared by multiple clients and multiple
 * servers.
 *
 * @author Quickstone Technologies Limited
 */
public interface Any2AnyConnection extends ConnectionWithSharedAltingClient, ConnectionWithSharedAltingServer {
    /**
     * Returns a reference to the client end of the connection for use by the client
     * processes.
     */
    @Override
    public SharedAltingConnectionClient client();

    /**
     * Returns a reference to the server end of the connection for use by the server
     * processes.
     */
    @Override
    public SharedConnectionServer server();
}
