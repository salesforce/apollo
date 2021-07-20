
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
 * Defines an interface for a connection that can be shared by multiple
 * concurrent clients but used by a single server. The server end of the
 * connection can be used as a guard in an <code>Alternative</code>.
 * </p>
 *
 * @author Quickstone Technologies Limited
 */
public interface Any2OneConnection extends ConnectionWithSharedAltingClient {
    /**
     * Returns a client end of the connection. This may only be safely used by a
     * single process but further calls will return new clients which may be used by
     * other processes.
     *
     * @return a new <code>SharedAltingConnectionClient</code> object.
     */
    @Override
    public SharedAltingConnectionClient client();

    /**
     * Returns the server end of the connection.
     *
     * @return the instance of the <code>AltingConnectionServer</code>.
     */
    public AltingConnectionServer server();
}
