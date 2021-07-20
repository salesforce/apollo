
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
 * This is used to create a buffered integer channel that always accepts and
 * never loses any input.
 * <H2>Description</H2> <TT>InfiniteBufferInt</TT> is an implementation of
 * <TT>ChannelDataStoreInt</TT> that yields a <I>FIFO</I> buffered semantics for
 * a channel. When empty, the channel blocks readers. However, its capacity is
 * <I>infinite</I> (expanding to whatever is needed so far as the underlying
 * memory system will permit). So, it <I>never</I> gets full and blocks a
 * writer. See the <tt>static</tt> construction methods of
 * {@link jcsp.lang.Channel}
 * ({@link jcsp.lang.Channel#one2oneInt(jcsp.util.ints.ChannelDataStoreInt)}
 * etc.).
 * <P>
 * The <TT>getState</TT> method returns <TT>EMPTY</TT> or <TT>NONEMPTYFULL</TT>,
 * but never <TT>FULL</TT>.
 * <P>
 * An initial size for the buffer can be specified during construction.
 *
 * @see ZeroBufferInt
 * @see BufferInt
 * @see jcsp.util.ints.OverWriteOldestBufferInt
 * @see jcsp.util.ints.OverWritingBufferInt
 * @see jcsp.util.ints.OverFlowingBufferInt
 * @see jcsp.lang.ChannelInt
 *
 * @author P.D. Austin
 */
//}}}

public class InfiniteBufferInt implements ChannelDataStoreInt, Serializable {
    /** The default size of the buffer */
    private static final int DEFAULT_SIZE = 8;

    /** The initial size of the buffer */
    private int initialSize;

    /** The storage for the buffered ints */
    private int[] buffer;

    /** The number of ints stored in the InfiniteBufferInt */
    private int counter = 0;

    /** The index of the oldest element (when counter > 0) */
    private int firstIndex = 0;

    /** The index of the next free element (when counter < buffer.length) */
    private int lastIndex = 0;

    /**
     * Construct a new <TT>InfiniteBufferInt</TT> with the default size (of 8).
     */
    public InfiniteBufferInt() {
        this(DEFAULT_SIZE);
    }

    /**
     * Construct a new <TT>InfiniteBufferInt</TT> with the specified initial size.
     *
     * @param initialSize the number of ints the <TT>InfiniteBufferInt</TT> can
     *                    initially store.
     * @throws BufferIntSizeError if <TT>initialSize</TT> is zero or negative. Note:
     *                            no action should be taken to
     *                            <TT>try</TT>/<TT>catch</TT> this exception -
     *                            application code generating it is in error and
     *                            needs correcting.
     */
    public InfiniteBufferInt(int initialSize) {
        if (initialSize <= 0)
            throw new BufferIntSizeError("\n*** Attempt to create a buffered channel with an initially negative or zero capacity");
        this.initialSize = initialSize;
        buffer = new int[initialSize];
    }

    /**
     * Returns the oldest <TT>int</TT> from the <TT>InfiniteBufferInt</TT> and
     * removes it.
     * <P>
     * <I>Pre-condition</I>: <TT>getState</TT> must not currently return
     * <TT>EMPTY</TT>.
     *
     * @return the oldest <TT>int</TT> from the <TT>InfiniteBufferInt</TT>
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
     * Puts a new <TT>int</TT> into the <TT>InfiniteBufferInt</TT>.
     * <P>
     * <I>Implementation note:</I> if <TT>InfiniteBufferInt</TT> is full, a new
     * internal buffer with double the capacity is constructed and the old data
     * copied across.
     *
     * @param value the int to put into the InfiniteBufferInt
     */
    @Override
    public void put(int value) {
        if (counter == buffer.length) {
            int[] temp = buffer;
            buffer = new int[buffer.length * 2];
            System.arraycopy(temp, firstIndex, buffer, 0, temp.length - firstIndex);
            System.arraycopy(temp, 0, buffer, temp.length - firstIndex, firstIndex);
            firstIndex = 0;
            lastIndex = temp.length;
        }
        buffer[lastIndex] = value;
        lastIndex = (lastIndex + 1) % buffer.length;
        counter++;
    }

    /**
     * Returns the current state of the <TT>InfiniteBufferInt</TT>.
     *
     * @return the current state of the <TT>InfiniteBufferInt</TT> (<TT>EMPTY</TT>
     *         or <TT>NONEMPTYFULL</TT>)
     */
    @Override
    public int getState() {
        if (counter == 0)
            return EMPTY;
        else
            return NONEMPTYFULL;
    }

    /**
     * Returns a new (and <TT>EMPTY</TT>) <TT>InfiniteBufferInt</TT> with the same
     * creation parameters as this one.
     * <P>
     * <I>Note: Only the initial size and structure of the
     * </I><TT>InfiniteBufferInt</TT><I> is cloned, not any stored data.</I>
     *
     * @return the cloned instance of this <TT>InfiniteBufferInt</TT>.
     */
    @Override
    public Object clone() {
        return new InfiniteBufferInt(initialSize);
    }

    @Override
    public void removeAll() {
        counter = 0;
        firstIndex = 0;
        lastIndex = 0;
    }
}
