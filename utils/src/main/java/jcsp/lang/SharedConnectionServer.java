
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
 * Defines an interface for a server end of a connection that can be shared by
 * multiple concurrent processes.
 * </p>
 * <p>
 * <code>SharedConnectionServer</code> objects cannot have their requests ALTed
 * over.
 * </p>
 *
 * @author Quickstone Technologies Limited
 */
public interface SharedConnectionServer<T> extends ConnectionServer<T> {
    /**
     * <p>
     * Creates a duplicate copy of the connection end.
     * </p>
     *
     * @return the duplicate <code>SharedConnectionServer</code> object.
     *
     */
    public SharedConnectionServer<T> duplicate();
}
