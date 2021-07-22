
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

package jcsp.util.ints;

import java.io.Serializable;

/**
 * This is used to create a buffered integer channel that always accepts input,
 * discarding its last entered data if full.
 * <H2>Description</H2> <TT>OverFlowingBufferInt</TT> is an implementation of
 * <TT>ChannelDataStoreInt</TT> that yields a <I>FIFO</I> buffered semantics for
 * a channel. When empty, the channel blocks readers. When full, a writer will
 * be accepted but the written value <I>overflows</I> the buffer and is lost to
 * the channel. See the <tt>static</tt> construction methods of
 * {@link jcsp.lang.Channel}
 * ({@link jcsp.lang.Channel#one2oneInt(ChannelDataStoreInt)} etc.).
 * <P>
 * The <TT>getState</TT> method returns <TT>EMPTY</TT> or <TT>NONEMPTYFULL</TT>,
 * but never <TT>FULL</TT>.
 *
 * @see ZeroBufferInt
 * @see BufferInt
 * @see jcsp.util.ints.OverWriteOldestBufferInt
 * @see jcsp.util.ints.OverWritingBufferInt
 * @see InfiniteBufferInt
 * @see jcsp.lang.ChannelInt
 *
 * @author P.D. Austin
 */

public class OverFlowingBufferInt implements ChannelDataStoreInt, Serializable {
    /** The storage for the buffered ints */
    private final int[] buffer;

    /** The number of ints stored in the Buffer */
    private int counter = 0;

    /** The index of the oldest element (when counter > 0) */
    private int firstIndex = 0;

    /** The index of the next free element (when counter < buffer.length) */
    private int lastIndex = 0;

    /**
     * Construct a new <TT>OverFlowingBufferInt</TT> with the specified size.
     *
     * @param size the number of ints the OverFlowingBufferInt can store.
     * @throws BufferIntSizeError if <TT>size</TT> is zero or negative. Note: no
     *                            action should be taken to
     *                            <TT>try</TT>/<TT>catch</TT> this exception -
     *                            application code generating it is in error and
     *                            needs correcting.
     */
    public OverFlowingBufferInt(int size) {
        if (size <= 0)
            throw new BufferIntSizeError("\n*** Attempt to create an overflowing buffered channel with negative or zero capacity");
        buffer = new int[size];
    }

    /**
     * Returns the oldest <TT>int</TT> from the <TT>OverFlowingBufferInt</TT> and
     * removes it.
     * <P>
     * <I>Pre-condition</I>: <TT>getState</TT> must not currently return
     * <TT>EMPTY</TT>.
     *
     * @return the oldest <TT>int</TT> from the <TT>OverFlowingBufferInt</TT>
     */
    @Override
    public int get() {
        int value = buffer[firstIndex];
        firstIndex = (firstIndex + 1) % buffer.length;
        counter--;
        return value;
    }

    /**
     * Returns the oldest integer from the buffer but does not remove it.
     * 
     * <I>Pre-condition</I>: <TT>getState</TT> must not currently return
     * <TT>EMPTY</TT>.
     *
     * @return the oldest <TT>int</TT> from the <TT>Buffer</TT>
     */
    @Override
    public int startGet() {
        return buffer[firstIndex];
    }

    /**
     * Removes the oldest integer from the buffer.
     */
    @Override
    public void endGet() {
        firstIndex = (firstIndex + 1) % buffer.length;
        counter--;
    }

    /**
     * Puts a new <TT>int</TT> into the <TT>OverFlowingBufferInt</TT>.
     * <P>
     * If <TT>OverFlowingBufferInt</TT> is full, the item is discarded.
     *
     * @param value the int to put into the OverFlowingBufferInt
     */
    @Override
    public void put(int value) {
        if (counter < buffer.length) {
            buffer[lastIndex] = value;
            lastIndex = (lastIndex + 1) % buffer.length;
            counter++;
        }
    }

    /**
     * Returns the current state of the <TT>OverFlowingBufferInt</TT>.
     *
     * @return the current state of the <TT>OverFlowingBufferInt</TT>
     *         (<TT>EMPTY</TT> or <TT>NONEMPTYFULL</TT>)
     */
    @Override
    public int getState() {
        if (counter == 0)
            return EMPTY;
        else
            return NONEMPTYFULL;
    }

    /**
     * Returns a new (and <TT>EMPTY</TT>) <TT>OverFlowingBufferInt</TT> with the
     * same creation parameters as this one.
     * <P>
     * <I>Note: Only the size and structure of the
     * </I><TT>OverFlowingBufferInt</TT><I> is cloned, not any stored data.</I>
     *
     * @return the cloned instance of this <TT>OverFlowingBufferInt</TT>.
     */
    @Override
    public Object clone() {
        return new OverFlowingBufferInt(buffer.length);
    }

    @Override
    public void removeAll() {
        counter = 0;
        firstIndex = 0;
        lastIndex = 0;
    }
}
