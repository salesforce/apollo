
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
 * Defines an interface for a factory that can create arrays of connections.
 *
 * @author Quickstone Technologies Limited
 */
public interface ConnectionArrayFactory {
    /**
     * Constructs and returns an array of instances of an implementation of
     * <code>One2OneConnection</code>.
     *
     * @param n the number of <code>One2OneConnection</code> objects to construct.
     *
     * @return the constructed array of <code>One2OneConnection</code> objects.
     */
    public One2OneConnection[] createOne2One(int n);

    /**
     * Constructs and returns an array of instances of an implementation of
     * <code>Any2OneConnection</code>.
     *
     * @param n the number of <code>Any2OneConnection</code> objects to construct.
     *
     * @return the constructed array of <code>Any2OneConnection</code> objects.
     */
    public Any2OneConnection[] createAny2One(int n);

    /**
     * Constructs and returns an array of instances of an implementation of
     * <code>One2AnyConnection</code>.
     *
     * @param n the number of <code>One2AnyConnection</code> objects to construct.
     *
     * @return the constructed array of <code>One2AnyConnection</code> objects.
     */
    public One2AnyConnection[] createOne2Any(int n);

    /**
     * Constructs and returns an array of instances of an implementation of
     * <code>Any2AnyConnection</code>.
     *
     * @param n the number of <code>Any2AnyConnection</code> objects to construct.
     *
     * @return the constructed array of <code>Any2AnyConnection</code> objects.
     */
    public Any2AnyConnection[] createAny2Any(int n);
}
