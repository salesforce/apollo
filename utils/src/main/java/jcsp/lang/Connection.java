
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
 * This class provides static factory methods for constructing different types
 * of connection. The methods are equivalent to the non-static methods of the
 * <code>StandardConnectionFactory</code> class.
 *
 * @author Quickstone Technologies Limited
 */
public class Connection {
    private static StandardConnectionFactory factory = new StandardConnectionFactory();

    /**
     * Constructor for Connection.
     */
    private Connection() {
        super();
    }

    /**
     * @see jcsp.lang.ConnectionFactory#createOne2One()
     */
    public static One2OneConnection createOne2One() {
        return factory.createOne2One();
    }

    /**
     * @see jcsp.lang.ConnectionFactory#createAny2One()
     */
    public static Any2OneConnection createAny2One() {
        return factory.createAny2One();
    }

    /**
     * @see jcsp.lang.ConnectionFactory#createOne2Any()
     */
    public static One2AnyConnection createOne2Any() {
        return factory.createOne2Any();
    }

    /**
     * @see jcsp.lang.ConnectionFactory#createAny2Any()
     */
    public static Any2AnyConnection createAny2Any() {
        return factory.createAny2Any();
    }

    /**
     * @see ConnectionArrayFactory#createOne2One(int)
     */
    public static One2OneConnection[] createOne2One(int n) {
        return factory.createOne2One(n);
    }

    /**
     * @see ConnectionArrayFactory#createAny2One(int)
     */
    public static Any2OneConnection[] any2oneArray(int n) {
        return factory.createAny2One(n);
    }

    /**
     * @see ConnectionArrayFactory#createOne2Any(int)
     */
    public static One2AnyConnection[] createOne2Any(int n) {
        return factory.createOne2Any(n);
    }

    /**
     * @see ConnectionArrayFactory#createAny2Any(int)
     */
    public static Any2AnyConnection[] createAny2Any(int n) {
        return factory.createAny2Any(n);
    }

    /**
     * Returns an array of client connection ends suitable for use as guards in an
     * <code>Alternative</code> construct.
     *
     * @param c the connection array to get the client ends from.
     * @return the array of client ends.
     */
    public static AltingConnectionClient[] getClientArray(One2AnyConnection[] c) {
        AltingConnectionClient r[] = new AltingConnectionClient[c.length];
        for (int i = 0; i < c.length; i++)
            r[i] = c[i].client();
        return r;
    }

    /**
     * Returns an array of client connection ends suitable for use as guards in an
     * <code>Alternative</code> construct.
     *
     * @param c the connection array to get the client ends from.
     * @return the array of client ends.
     */
    public static AltingConnectionClient[] getClientArray(One2OneConnection[] c) {
        AltingConnectionClient r[] = new AltingConnectionClient[c.length];
        for (int i = 0; i < c.length; i++)
            r[i] = c[i].client();
        return r;
    }

    /**
     * Returns an array of client connection ends suitable for use by multiple
     * concurrent processes.
     *
     * @param c the connection array to get the client ends from.
     * @return the array of client ends.
     */
    public static SharedConnectionClient[] getClientArray(Any2AnyConnection[] c) {
        SharedConnectionClient r[] = new SharedConnectionClient[c.length];
        for (int i = 0; i < c.length; i++)
            r[i] = c[i].client();
        return r;
    }

    /**
     * Returns an array of client connection ends suitable for use by multiple
     * concurrent processes.
     *
     * @param c the connection array to get the client ends from.
     * @return the array of client ends.
     */
    public static SharedConnectionClient[] getClientArray(Any2OneConnection[] c) {
        SharedConnectionClient r[] = new SharedConnectionClient[c.length];
        for (int i = 0; i < c.length; i++)
            r[i] = c[i].client();
        return r;
    }

    /**
     * Returns an array of server connection ends suitable for use as guards in an
     * <code>Alternative</code> construct.
     *
     * @param c the connection array to get the server ends from.
     * @return the array of server ends.
     */
    public static AltingConnectionServer[] getServerArray(Any2OneConnection[] c) {
        AltingConnectionServer r[] = new AltingConnectionServer[c.length];
        for (int i = 0; i < c.length; i++)
            r[i] = c[i].server();
        return r;
    }

    /**
     * Returns an array of server connection ends suitable for use as guards in an
     * <code>Alternative</code> construct.
     *
     * @param c the connection array to get the server ends from.
     * @return the array of server ends.
     */
    public static AltingConnectionServer[] getServerArray(One2OneConnection[] c) {
        AltingConnectionServer r[] = new AltingConnectionServer[c.length];
        for (int i = 0; i < c.length; i++)
            r[i] = c[i].server();
        return r;
    }

    /**
     * Returns an array of server connection ends suitable for use by multiple
     * concurrent processes.
     *
     * @param c the connection array to get the server ends from.
     * @return the array of server ends.
     */
    public static SharedConnectionServer[] getServerArray(Any2AnyConnection[] c) {
        SharedConnectionServer r[] = new SharedConnectionServer[c.length];
        for (int i = 0; i < c.length; i++)
            r[i] = c[i].server();
        return r;
    }

    /**
     * Returns an array of server connection ends suitable for use by multiple
     * concurrent processes.
     *
     * @param c the connection array to get the server ends from.
     * @return the array of server ends.
     */
    public static SharedConnectionServer[] getServerArray(One2AnyConnection[] c) {
        SharedConnectionServer r[] = new SharedConnectionServer[c.length];
        for (int i = 0; i < c.length; i++)
            r[i] = c[i].server();
        return r;
    }
}
