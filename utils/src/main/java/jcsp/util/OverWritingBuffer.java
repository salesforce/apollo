
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
 * This is used to create a buffered object channel that always accepts input,
 * overwriting its last entered data if full.
 * <H2>Description</H2> It is an implementation of <TT>ChannelDataStore</TT>
 * that yields a <I>FIFO</I> buffered semantics for a channel. When empty, the
 * channel blocks readers. When full, a writer will overwrite the <I>latest</I>
 * item written to the channel. See the <tt>static</tt> construction methods of
 * {@link jcsp.lang.Channel}
 * ({@link jcsp.lang.Channel#one2one(ChannelDataStore)} etc.).
 * <P>
 * The <TT>getState</TT> method returns <TT>EMPTY</TT> or <TT>NONEMPTYFULL</TT>,
 * but never <TT>FULL</TT>.
 *
 * @see ZeroBuffer
 * @see Buffer
 * @see jcsp.util.OverWriteOldestBuffer
 * @see OverFlowingBuffer
 * @see InfiniteBuffer
 * @see jcsp.lang.Channel
 *
 * @author P.D. Austin
 */
//}}}

public class OverWritingBuffer<T> implements ChannelDataStore<T>, Serializable {
    private static final long serialVersionUID = 1L;
    /** The storage for the buffered Objects */
    private final T[]         buffer;

    /** The number of Objects stored in the Buffer */
    private int counter = 0;

    /** The index of the oldest element (when counter &gt; 0) */
    private int firstIndex = 0;

    /** The index of the next free element (when counter &lt; buffer.length) */
    private int lastIndex = 0;

    private boolean valueWrittenWhileFull = false;

    /**
     * Construct a new <TT>OverWritingBuffer</TT> with the specified size.
     *
     * @param size the number of Objects the OverWritingBuffer can store.
     * @throws BufferSizeError if <TT>size</TT> is zero or negative. Note: no action
     *                         should be taken to <TT>try</TT>/<TT>catch</TT> this
     *                         exception - application code generating it is in
     *                         error and needs correcting.
     */
    @SuppressWarnings("unchecked")
    public OverWritingBuffer(int size) {
        if (size <= 0)
            throw new BufferSizeError("\n*** Attempt to create an overwriting buffered channel with negative or zero capacity");
        buffer = (T[]) new Object[size];
    }

    /**
     * Returns the oldest <TT>Object</TT> from the <TT>OverWritingBuffer</TT> and
     * removes it.
     * <P>
     * <I>Pre-condition</I>: <TT>getState</TT> must not currently return
     * <TT>EMPTY</TT>.
     *
     * @return the oldest <TT>Object</TT> from the <TT>OverWritingBuffer</TT>
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
     * Puts a new <TT>Object</TT> into the <TT>OverWritingBuffer</TT>.
     * <P>
     * If <TT>OverWritingBuffer</TT> is full, the last item previously put into the
     * buffer will be overwritten.
     *
     * @param value the Object to put into the OverWritingBuffer
     */
    @Override
    public void put(T value) {
        if (counter == buffer.length) {
            buffer[(lastIndex - 1 + buffer.length) % buffer.length] = value;
            valueWrittenWhileFull = true;
        } else {
            buffer[lastIndex] = value;
            lastIndex = (lastIndex + 1) % buffer.length;
            counter++;
        }
    }

    /**
     * This begins an extended rendezvous by the reader. The semantics of an
     * extended rendezvous on an overwrite-newest buffer are slightly complicated,
     * but hopefully intuitive.
     * <p>
     * If the buffer is of size 2 or larger, the semantics are as follows. Beginning
     * an extended rendezvous will return the oldest value in the buffer, but not
     * remove it. If the writer writes to the buffer during the rendezvous, it will
     * grow the buffer and end up overwriting the newest value as normal. At the end
     * of the extended rendezvous, the oldest value is removed.
     * <p>
     * If the buffer is of size 1, the semantics are identical to those of an
     * {@link OverWriteOldestBuffer}. For a complete description, refer to the
     * documentation for the {@link OverWriteOldestBuffer#startGet()} method.
     * 
     * @return The oldest value in the buffer at this time
     */
    @Override
    public T startGet() {
        valueWrittenWhileFull = false;
        return buffer[firstIndex];
    }

    /**
     * This ends an extended rendezvous by the reader.
     * 
     * @see #startGet()
     */
    @Override
    public void endGet() {
        if (!valueWrittenWhileFull || buffer.length != 1) {
            // Our data hasn't been over-written so remove it:
            buffer[firstIndex] = null;
            firstIndex = (firstIndex + 1) % buffer.length;
            counter--;
        }
    }

    /**
     * Returns the current state of the <TT>OverWritingBuffer</TT>.
     *
     * @return the current state of the <TT>OverWritingBuffer</TT> (<TT>EMPTY</TT>
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
     * Returns a new (and <TT>EMPTY</TT>) <TT>OverWritingBuffer</TT> with the same
     * creation parameters as this one.
     * <P>
     * <I>Note: Only the size and structure of the </I><TT>OverWritingBuffer</TT><I>
     * is cloned, not any stored data.</I>
     *
     * @return the cloned instance of this <TT>OverWritingBuffer</TT>.
     */
    @Override
    public OverWritingBuffer<T> clone() {
        return new OverWritingBuffer<T>(buffer.length);
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
