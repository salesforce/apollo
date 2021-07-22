
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
 * Implements a factory for creating connections.
 * </p>
 *
 * @author Quickstone Technologies Limited
 */
public class StandardConnectionFactory implements ConnectionFactory, ConnectionArrayFactory {
    /**
     * @see ConnectionFactory#createOne2One
     */
    @Override
    public One2OneConnection createOne2One() {
        return new One2OneConnectionImpl();
    }

    /**
     * @see ConnectionFactory#createAny2One
     */
    @Override
    public Any2OneConnection createAny2One() {
        return new Any2OneConnectionImpl();
    }

    /**
     * @see ConnectionFactory#createOne2Any
     */
    @Override
    public One2AnyConnection createOne2Any() {
        return new One2AnyConnectionImpl();
    }

    /**
     * @see ConnectionFactory#createAny2Any
     */
    @Override
    public Any2AnyConnection createAny2Any() {
        return new Any2AnyConnectionImpl();
    }

    /**
     * @see ConnectionArrayFactory#createOne2One
     */
    @Override
    public One2OneConnection[] createOne2One(int n) {
        One2OneConnection[] toReturn = new One2OneConnection[n];
        for (int i = 0; i < n; i++)
            toReturn[i] = createOne2One();
        return toReturn;
    }

    /**
     * @see ConnectionArrayFactory#createAny2One
     */
    @Override
    public Any2OneConnection[] createAny2One(int n) {
        Any2OneConnection[] toReturn = new Any2OneConnection[n];
        for (int i = 0; i < n; i++)
            toReturn[i] = createAny2One();
        return toReturn;
    }

    /**
     * @see ConnectionArrayFactory#createOne2Any
     */
    @Override
    public One2AnyConnection[] createOne2Any(int n) {
        One2AnyConnection[] toReturn = new One2AnyConnection[n];
        for (int i = 0; i < n; i++)
            toReturn[i] = createOne2Any();
        return toReturn;
    }

    /**
     * @see ConnectionArrayFactory#createAny2Any
     */
    @Override
    public Any2AnyConnection[] createAny2Any(int n) {
        Any2AnyConnection[] toReturn = new Any2AnyConnection[n];
        for (int i = 0; i < n; i++)
            toReturn[i] = createAny2Any();
        return toReturn;
    }
}
