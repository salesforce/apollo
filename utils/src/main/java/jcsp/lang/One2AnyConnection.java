
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
 * An interface for a connection which can be used by only one client but which can be used by multiple
 * concurrent servers.
 *
 * @author Quickstone Technologies Limited
 */
public interface One2AnyConnection extends ConnectionWithSharedAltingServer
{
    /**
     * Returns the client part of the connection.
     */
    public AltingConnectionClient client();

    /**
     * Returns the server part of the connection.
     */
    public SharedConnectionServer server();
}
