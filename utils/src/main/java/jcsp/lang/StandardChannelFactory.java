
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

import jcsp.util.ChannelDataStore;

/**
 * <p>
 * This class acts as a Factory for creating channels. It can create
 * non-buffered and buffered channels and also arrays of non-buffered and
 * buffered channels.
 * </p>
 *
 * <p>
 * The Channel objects created by this Factory are formed of separate objects
 * for the read and write ends. Therefore the <code>ChannelInput</code> object
 * cannot be cast into the <code>ChannelOutput</code> object and vice-versa.
 * </p>
 *
 * <p>
 * The current implementation uses an instance of the
 * <code>RiskyChannelFactory</code> to construct the underlying raw channels.
 * </p>
 *
 * @author Quickstone Technologies Limited
 */
public class StandardChannelFactory implements ChannelFactory, ChannelArrayFactory, BufferedChannelFactory,
                                    BufferedChannelArrayFactory {
    private static StandardChannelFactory defaultInstance = new StandardChannelFactory();

    /**
     * Constructs a new factory.
     */
    public StandardChannelFactory() {
        super();
    }

    /**
     * Returns a default instance of a channel factory.
     */
    public static StandardChannelFactory getDefaultInstance() {
        return defaultInstance;
    }

    /**
     * Constructs and returns a <code>One2OneChannel</code> object.
     *
     * @return the channel object.
     *
     * @see ChannelFactory#createOne2One()
     */
    @Override
    public One2OneChannel createOne2One() {
        return new One2OneChannelImpl();
    }

    /**
     * Constructs and returns an <code>Any2OneChannel</code> object.
     *
     * @return the channel object.
     *
     * @see ChannelFactory#createAny2One()
     */
    @Override
    public Any2OneChannel createAny2One() {
        return new Any2OneChannelImpl();
    }

    /**
     * Constructs and returns a <code>One2AnyChannel</code> object.
     *
     * @return the channel object.
     *
     * @see ChannelFactory#createOne2Any()
     */
    @Override
    public One2AnyChannel createOne2Any() {
        return new One2AnyChannelImpl();
    }

    /**
     * Constructs and returns an <code>Any2AnyChannel</code> object.
     *
     * @return the channel object.
     *
     * @see ChannelFactory#createAny2Any()
     */
    @Override
    public Any2AnyChannel createAny2Any() {
        return new Any2AnyChannelImpl();
    }

    /**
     * Constructs and returns an array of <code>One2OneChannel</code> objects.
     *
     * @param n the size of the array of channels.
     * @return the array of channels.
     *
     * @see ChannelArrayFactory#createOne2One(int)
     */
    @Override
    public One2OneChannel[] createOne2One(int n) {
        One2OneChannel[] toReturn = new One2OneChannel[n];
        for (int i = 0; i < n; i++)
            toReturn[i] = createOne2One();
        return toReturn;
    }

    /**
     * Constructs and returns an array of <code>Any2OneChannel</code> objects.
     *
     * @param n the size of the array of channels.
     * @return the array of channels.
     *
     * @see ChannelArrayFactory#createAny2One(int)
     */
    @Override
    public Any2OneChannel[] createAny2One(int n) {
        Any2OneChannel[] toReturn = new Any2OneChannel[n];
        for (int i = 0; i < n; i++)
            toReturn[i] = createAny2One();
        return toReturn;
    }

    /**
     * Constructs and returns an array of <code>One2AnyChannel</code> objects.
     *
     * @param n the size of the array of channels.
     * @return the array of channels.
     *
     * @see ChannelArrayFactory#createOne2Any(int)
     */
    @Override
    public One2AnyChannel[] createOne2Any(int n) {
        One2AnyChannel[] toReturn = new One2AnyChannel[n];
        for (int i = 0; i < n; i++)
            toReturn[i] = createOne2Any();
        return toReturn;
    }

    /**
     * Constructs and returns an array of <code>Any2AnyChannel</code> objects.
     *
     * @param n the size of the array of channels.
     * @return the array of channels.
     *
     * @see ChannelArrayFactory#createAny2Any(int)
     */
    @Override
    public Any2AnyChannel[] createAny2Any(int n) {
        Any2AnyChannel[] toReturn = new Any2AnyChannel[n];
        for (int i = 0; i < n; i++)
            toReturn[i] = createAny2Any();
        return toReturn;
    }

    /**
     * <p>
     * Constructs and returns a <code>One2OneChannel</code> object which uses the
     * specified <code>ChannelDataStore</code> object as a buffer.
     * </p>
     * <p>
     * The buffer supplied to this method is cloned before it is inserted into the
     * channel.
     * </p>
     *
     * @param buffer the <code>ChannelDataStore</code> to use.
     * @return the buffered channel.
     *
     * @see BufferedChannelFactory#createOne2One(ChannelDataStore)
     * @see ChannelDataStore
     */
    @Override
    public One2OneChannel createOne2One(ChannelDataStore buffer) {
        return new BufferedOne2OneChannel(buffer);
    }

    /**
     * <p>
     * Constructs and returns a <code>Any2OneChannel</code> object which uses the
     * specified <code>ChannelDataStore</code> object as a buffer.
     * </p>
     * <p>
     * The buffer supplied to this method is cloned before it is inserted into the
     * channel.
     * </p>
     *
     * @param buffer the <code>ChannelDataStore</code> to use.
     * @return the buffered channel.
     *
     * @see BufferedChannelFactory#createAny2One(ChannelDataStore)
     * @see ChannelDataStore
     */
    @Override
    public Any2OneChannel createAny2One(ChannelDataStore buffer) {
        return new BufferedAny2OneChannel(buffer);
    }

    /**
     * <p>
     * Constructs and returns a <code>One2AnyChannel</code> object which uses the
     * specified <code>ChannelDataStore</code> object as a buffer.
     * </p>
     * <p>
     * The buffer supplied to this method is cloned before it is inserted into the
     * channel.
     * </p>
     *
     * @param buffer the <code>ChannelDataStore</code> to use.
     * @return the buffered channel.
     *
     * @see BufferedChannelFactory#createOne2Any(ChannelDataStore)
     * @see ChannelDataStore
     */
    @Override
    public One2AnyChannel createOne2Any(ChannelDataStore buffer) {
        return new BufferedOne2AnyChannel(buffer);
    }

    /**
     * <p>
     * Constructs and returns a <code>Any2AnyChannel</code> object which uses the
     * specified <code>ChannelDataStore</code> object as a buffer.
     * </p>
     * <p>
     * The buffer supplied to this method is cloned before it is inserted into the
     * channel.
     * </p>
     *
     * @param buffer the <code>ChannelDataStore</code> to use.
     * @return the buffered channel.
     *
     * @see BufferedChannelFactory#createAny2Any(ChannelDataStore)
     * @see ChannelDataStore
     */
    @Override
    public Any2AnyChannel createAny2Any(ChannelDataStore buffer) {
        return new BufferedAny2AnyChannel(buffer);
    }

    /**
     * <p>
     * Constructs and returns an array of <code>One2OneChannel</code> objects which
     * use the specified <code>ChannelDataStore</code> object as a buffer.
     * </p>
     * <p>
     * The buffer supplied to this method is cloned before it is inserted into the
     * channel. This is why an array of buffers is not required.
     * </p>
     *
     * @param buffer the <code>ChannelDataStore</code> to use.
     * @param n      the size of the array of channels.
     * @return the array of buffered channels.
     *
     * @see BufferedChannelArrayFactory#createOne2One(ChannelDataStore,int)
     * @see ChannelDataStore
     */
    @Override
    public One2OneChannel[] createOne2One(ChannelDataStore buffer, int n) {
        One2OneChannel[] toReturn = new One2OneChannel[n];
        for (int i = 0; i < n; i++)
            toReturn[i] = createOne2One(buffer);
        return toReturn;
    }

    /**
     * <p>
     * Constructs and returns an array of <code>Any2OneChannel</code> objects which
     * use the specified <code>ChannelDataStore</code> object as a buffer.
     * </p>
     * <p>
     * The buffer supplied to this method is cloned before it is inserted into the
     * channel. This is why an array of buffers is not required.
     * </p>
     *
     * @param buffer the <code>ChannelDataStore</code> to use.
     * @param n      the size of the array of channels.
     * @return the array of buffered channels.
     *
     * @see BufferedChannelArrayFactory#createAny2One(ChannelDataStore,int)
     * @see ChannelDataStore
     */
    @Override
    public Any2OneChannel[] createAny2One(ChannelDataStore buffer, int n) {
        Any2OneChannel[] toReturn = new Any2OneChannel[n];
        for (int i = 0; i < n; i++)
            toReturn[i] = createAny2One(buffer);
        return toReturn;
    }

    /**
     * <p>
     * Constructs and returns an array of <code>One2AnyChannel</code> objects which
     * use the specified <code>ChannelDataStore</code> object as a buffer.
     * </p>
     * <p>
     * The buffer supplied to this method is cloned before it is inserted into the
     * channel. This is why an array of buffers is not required.
     * </p>
     *
     * @param buffer the <code>ChannelDataStore</code> to use.
     * @param n      the size of the array of channels.
     * @return the array of buffered channels.
     *
     * @see BufferedChannelArrayFactory#createOne2Any(ChannelDataStore,int)
     * @see ChannelDataStore
     */
    @Override
    public One2AnyChannel[] createOne2Any(ChannelDataStore buffer, int n) {
        One2AnyChannel[] toReturn = new One2AnyChannel[n];
        for (int i = 0; i < n; i++)
            toReturn[i] = createOne2Any(buffer);
        return toReturn;
    }

    /**
     * <p>
     * Constructs and returns an array of <code>Any2AnyChannel</code> objects which
     * use the specified <code>ChannelDataStore</code> object as a buffer.
     * </p>
     * <p>
     * The buffer supplied to this method is cloned before it is inserted into the
     * channel. This is why an array of buffers is not required.
     * </p>
     *
     * @param buffer the <code>ChannelDataStore</code> to use.
     * @param n      the size of the array of channels.
     * @return the array of buffered channels.
     *
     * @see BufferedChannelArrayFactory#createAny2Any(ChannelDataStore,int)
     * @see ChannelDataStore
     */
    @Override
    public Any2AnyChannel[] createAny2Any(ChannelDataStore buffer, int n) {
        Any2AnyChannel[] toReturn = new Any2AnyChannel[n];
        for (int i = 0; i < n; i++)
            toReturn[i] = createAny2Any(buffer);
        return toReturn;
    }
}
