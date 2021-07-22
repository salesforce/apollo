
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

package jcsp.util;

import java.io.Serializable;

//{{{  javadoc
/**
 * This is used to create a zero-buffered object channel that never loses data.
 * <H2>Description</H2> <TT>ZeroBuffer</TT> is an implementation of
 * <TT>ChannelDataStore</TT> that yields the standard <I><B>CSP</B></I>
 * semantics for a channel -- that is zero buffered with direct synchronisation
 * between reader and writer. Unless specified otherwise, this is the default
 * behaviour for channels. See the <tt>static</tt> construction methods of
 * {@link jcsp.lang.Channel}
 * ({@link jcsp.lang.Channel#one2one(jcsp.util.ChannelDataStore)} etc.).
 * <P>
 * The <TT>getState</TT> method will return <TT>FULL</TT> if there is an output
 * waiting on the channel and <TT>EMPTY</TT> if there is not.
 *
 * @see Buffer
 * @see jcsp.util.OverWriteOldestBuffer
 * @see jcsp.util.OverWritingBuffer
 * @see jcsp.util.OverFlowingBuffer
 * @see jcsp.util.InfiniteBuffer
 * @see jcsp.lang.One2OneChannelImpl
 * @see jcsp.lang.Any2OneChannelImpl
 * @see jcsp.lang.One2AnyChannelImpl
 * @see jcsp.lang.Any2AnyChannelImpl
 *
 * @author P.D. Austin
 */
//}}}

public class ZeroBuffer<T> implements ChannelDataStore<T>, Serializable {
    private static final long serialVersionUID = 1L;
    /** The current state */
    private int               state            = EMPTY;

    /** The Object */
    private T value;

    /**
     * Returns the <TT>Object</TT> from the <TT>ZeroBuffer</TT>.
     * <P>
     * <I>Pre-condition</I>: <TT>getState</TT> must not currently return
     * <TT>EMPTY</TT>.
     *
     * @return the <TT>Object</TT> from the <TT>ZeroBuffer</TT>
     */
    @Override
    public T get() {
        state = EMPTY;
        T o = value;
        value = null;
        return o;
    }

    /**
     * Begins an extended rendezvous - simply returns the next object in the buffer.
     * This function does not remove the object.
     * 
     * <I>Pre-condition</I>: <TT>getState</TT> must not currently return
     * <TT>EMPTY</TT>.
     * 
     * @return The object in the buffer.
     */
    @Override
    public T startGet() {
        return value;
    }

    /**
     * Ends the extended rendezvous by clearing the buffer.
     */
    @Override
    public void endGet() {
        value = null;
        state = EMPTY;
    }

    /**
     * Puts a new <TT>Object</TT> into the <TT>ZeroBuffer</TT>.
     * <P>
     * <I>Pre-condition</I>: <TT>getState</TT> must not currently return
     * <TT>FULL</TT>.
     *
     * @param value the Object to put into the ZeroBuffer
     */
    @Override
    public void put(T value) {
        state = FULL;
        this.value = value;
    }

    /**
     * Returns the current state of the <TT>ZeroBuffer</TT>.
     *
     * @return the current state of the <TT>ZeroBuffer</TT> (<TT>EMPTY</TT> or
     *         <TT>FULL</TT>)
     */
    @Override
    public int getState() {
        return state;
    }

    /**
     * Returns a new (and <TT>EMPTY</TT>) <TT>ZeroBuffer</TT> with the same creation
     * parameters as this one.
     * <P>
     * <I>Note: Only the size and structure of the </I><TT>ZeroBuffer</TT><I> is
     * cloned, not any stored data.</I>
     *
     * @return the cloned instance of this <TT>ZeroBuffer</TT>.
     */
    @Override
    public ZeroBuffer<T> clone() {
        return new ZeroBuffer<T>();
    }

    @Override
    public void removeAll() {
        state = EMPTY;
        value = null;
    }
}
