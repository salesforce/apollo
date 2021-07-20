
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

/**
 * This is used to create a buffered object channel that never loses data.
 * <H2>Description</H2> <TT>Buffer</TT> is an implementation of
 * <TT>ChannelDataStore</TT> that yields a blocking <I>FIFO</I> buffered
 * semantics for a channel. See the <tt>static</tt> construction methods of
 * {@link jcsp.lang.Channel}
 * ({@link jcsp.lang.Channel#one2one(jcsp.util.ChannelDataStore)} etc.).
 * <P>
 * The <TT>getState</TT> method returns <TT>EMPTY</TT>, <TT>NONEMPTYFULL</TT> or
 * <TT>FULL</TT> according to the state of the buffer.
 *
 * @see jcsp.util.ZeroBuffer
 * @see jcsp.util.OverWriteOldestBuffer
 * @see jcsp.util.OverWritingBuffer
 * @see jcsp.util.OverFlowingBuffer
 * @see jcsp.util.InfiniteBuffer
 * @see jcsp.lang.Channel
 *
 * @author P.D. Austin
 */

public class Buffer<T> implements ChannelDataStore<T>, Serializable {
    private static final long serialVersionUID = 1L;

    /** The storage for the buffered Objects */
    private final T[] buffer;

    /** The number of Objects stored in the Buffer */
    private int counter = 0;

    /** The index of the oldest element (when counter > 0) */
    private int firstIndex = 0;

    /** The index of the next free element (when counter < buffer.length) */
    private int lastIndex = 0;

    /**
     * Construct a new <TT>Buffer</TT> with the specified size.
     *
     * @param size the number of Objects the Buffer can store.
     * @throws BufferSizeError if <TT>size</TT> is negative. Note: no action should
     *                         be taken to <TT>try</TT>/<TT>catch</TT> this
     *                         exception - application code generating it is in
     *                         error and needs correcting.
     */
    @SuppressWarnings("unchecked")
    public Buffer(int size) {
        if (size < 0)
            throw new BufferSizeError("\n*** Attempt to create a buffered channel with negative capacity");
        buffer = (T[]) new Object[size + 1]; // the extra one is a subtlety needed by
        // the current channel algorithms.
    }

    /**
     * Returns the oldest <TT>Object</TT> from the <TT>Buffer</TT> and removes it.
     * <P>
     * <I>Pre-condition</I>: <TT>getState</TT> must not currently return
     * <TT>EMPTY</TT>.
     *
     * @return the oldest <TT>Object</TT> from the <TT>Buffer</TT>
     */
    @Override
    public T get() {
        T value = buffer[firstIndex];
        buffer[firstIndex] = null;
        firstIndex = (firstIndex + 1) % buffer.length;
        counter--;
        return value;
    }

    /**
     * Returns the oldest object from the buffer but does not remove it.
     * 
     * <I>Pre-condition</I>: <TT>getState</TT> must not currently return
     * <TT>EMPTY</TT>.
     *
     * @return the oldest <TT>Object</TT> from the <TT>Buffer</TT>
     */
    @Override
    public T startGet() {
        return buffer[firstIndex];
    }

    /**
     * Removes the oldest object from the buffer.
     */
    @Override
    public void endGet() {
        buffer[firstIndex] = null;
        firstIndex = (firstIndex + 1) % buffer.length;
        counter--;
    }

    /**
     * Puts a new <TT>Object</TT> into the <TT>Buffer</TT>.
     * <P>
     * <I>Pre-condition</I>: <TT>getState</TT> must not currently return
     * <TT>FULL</TT>.
     *
     * @param value the Object to put into the Buffer
     */
    @Override
    public void put(T value) {
        buffer[lastIndex] = value;
        lastIndex = (lastIndex + 1) % buffer.length;
        counter++;
    }

    /**
     * Returns the current state of the <TT>Buffer</TT>.
     *
     * @return the current state of the <TT>Buffer</TT> (<TT>EMPTY</TT>,
     *         <TT>NONEMPTYFULL</TT> or <TT>FULL</TT>)
     */
    @Override
    public int getState() {
        if (counter == 0)
            return EMPTY;
        else if (counter == buffer.length)
            return FULL;
        else
            return NONEMPTYFULL;
    }

    /**
     * Returns a new (and <TT>EMPTY</TT>) <TT>Buffer</TT> with the same creation
     * parameters as this one.
     * <P>
     * <I>Note: Only the size and structure of the </I><TT>Buffer</TT><I> is cloned,
     * not any stored data.</I>
     *
     * @return the cloned instance of this <TT>Buffer</TT>
     */
    @Override
    public Buffer<T> clone() {
        return new Buffer<T>(buffer.length - 1);
    }

    @Override
    public void removeAll() {
        counter = 0;
        firstIndex = 0;
        lastIndex = 0;

        for (int i = 0; i < buffer.length; i++) {
            // Null the objects so they can be garbage collected:
            buffer[i] = null;
        }
    }
}
