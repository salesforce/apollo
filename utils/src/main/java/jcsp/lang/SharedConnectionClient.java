
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
 * Defines an interface for a client end of a connection that can be shared by
 * multiple clients.
 * </p>
 * <p>
 * This object cannot itself be shared between concurrent processes but
 * duplicate objects can be generated that can be used by multiple concurrent
 * processes. This can be achieved using the <code>{@link #duplicate()}</code>
 * method.
 * </p>
 * <p>
 * See <code>{@link ConnectionClient}</code> for a fuller explanation of how to
 * use connection client objects.
 * </p>
 *
 * @author Quickstone Technologies Limited
 */
public interface SharedConnectionClient<T> extends ConnectionClient<T, T> {
    /**
     * Returns a duplicates <code>SharedConnectionClient</code> object which may be
     * used by another process to this instance.
     *
     * @return a duplicate <code>SharedConnectionClient</code> object.
     */
    public SharedConnectionClient<T> duplicate();
}
