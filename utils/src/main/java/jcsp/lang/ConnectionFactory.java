
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
 * Defines an interface for a factory than can create connections.
 * </p>
 *
 * @author Quickstone Technologies Limited
 */
public interface ConnectionFactory {
    /**
     * Constructs and returns an implementation of <code>One2OneConnection</code>.
     *
     * @return the constructed <code>One2OneConnection</code> object.
     */
    public One2OneConnection createOne2One();

    /**
     * Constructs and returns an implementation of <code>Any2OneConnection</code>.
     *
     * @return the constructed <code>Any2OneConnection</code> object.
     */
    public Any2OneConnection createAny2One();

    /**
     * Constructs and returns an implementation of <code>One2AnyConnection</code>.
     *
     * @return the constructed <code>One2AnyConnection</code> object.
     */
    public One2AnyConnection createOne2Any();

    /**
     * Constructs and returns an implementation of <code>Any2AnyConnection</code>.
     *
     * @return the constructed <code>Any2AnyConnection</code> object.
     */
    public Any2AnyConnection createAny2Any();
}
