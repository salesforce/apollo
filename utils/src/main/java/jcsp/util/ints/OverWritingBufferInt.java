
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

import jcsp.util.OverWriteOldestBuffer;

//{{{  javadoc
/**
 * This is used to create a buffered integer channel that always accepts input,
 * overwriting its last entered data if full.
 * <H2>Description</H2> <TT>OverWritingBufferInt</TT> is an implementation of
 * <TT>ChannelDataStoreInt</TT> that yields a <I>FIFO</I> buffered semantics for
 * a channel. When empty, the channel blocks readers. When full, a writer will
 * overwrite the <I>latest</I> item written to the channel. See the
 * <tt>static</tt> construction methods of {@link jcsp.lang.Channel}
 * ({@link jcsp.lang.Channel#one2oneInt(ChannelDataStoreInt)} etc.).
 * <P>
 * The <TT>getState</TT> method returns <TT>EMPTY</TT> or <TT>NONEMPTYFULL</TT>,
 * but never <TT>FULL</TT>.
 *
 * @see ZeroBufferInt
 * @see BufferInt
 * @see jcsp.util.ints.OverWriteOldestBufferInt
 * @see OverFlowingBufferInt
 * @see InfiniteBufferInt
 * @see jcsp.lang.ChannelInt
 *
 * @author P.D. Austin
 */
//}}}

public class OverWritingBufferInt implements ChannelDataStoreInt, Serializable {
    /** The storage for the buffered ints */
    private final int[] buffer;

    /** The number of ints stored in the Buffer */
    private int counter = 0;

    /** The index of the oldest element (when counter > 0) */
    private int firstIndex = 0;

    /** The index of the next free element (when counter < buffer.length) */
    private int lastIndex = 0;

    private boolean valueWrittenWhileFull = false;

    /**
     * Construct a new <TT>OverWritingBufferInt</TT> with the specified size.
     *
     * @param size the number of ints the OverWritingBufferInt can store.
     * @throws BufferIntSizeError if <TT>size</TT> is zero or negative. Note: no
     *                            action should be taken to
     *                            <TT>try</TT>/<TT>catch</TT> this exception -
     *                            application code generating it is in error and
     *                            needs correcting.
     */
    public OverWritingBufferInt(int size) {
        if (size <= 0)
            throw new BufferIntSizeError("\n*** Attempt to create an overwriting buffered channel with negative or zero capacity");
        buffer = new int[size];
    }

    /**
     * Returns the oldest <TT>int</TT> from the <TT>OverWritingBufferInt</TT> and
     * removes it.
     * <P>
     * <I>Pre-condition</I>: <TT>getState</TT> must not currently return
     * <TT>EMPTY</TT>.
     *
     * @return the oldest <TT>int</TT> from the <TT>OverWritingBufferInt</TT>
     */
    @Override
    public int get() {
        int value = buffer[firstIndex];
        firstIndex = (firstIndex + 1) % buffer.length;
        counter--;
        return value;
    }

    /**
     * Begins an extended rendezvous by the reader.
     * 
     * The semantics of an extended rendezvous on an overwrite-newest buffer are
     * slightly complicated, but hopefully intuitive.
     * 
     * If the buffer is of size 2 or larger, the semantics are as follows. Beginning
     * an extended rendezvous will return the oldest value in the buffer, but not
     * remove it. If the writer writes to the buffer during the rendezvous, it will
     * grow the buffer and end up overwriting the newest value as normal. At the end
     * of the extended rendezvous, the oldest value is removed.
     * 
     * If the buffer is of size 1, the semantics are identical to those of an
     * {@link OverWriteOldestBuffer}. For a complete description, refer to the
     * documentation for the {@link OverWriteOldestBuffer#startGet()} method.
     * 
     * @return The oldest value in the buffer at this time
     */
    @Override
    public int startGet() {
        valueWrittenWhileFull = false;
        return buffer[firstIndex];
    }

    /**
     * See {@link #startGet()} for a description of the semantics of this method.
     */
    @Override
    public void endGet() {
        if (!valueWrittenWhileFull || buffer.length != 1) {
            // Our data hasn't been over-written so remove it:
            firstIndex = (firstIndex + 1) % buffer.length;
            counter--;
        }
    }

    /**
     * Puts a new <TT>int</TT> into the <TT>OverWritingBufferInt</TT>.
     * <P>
     * If <TT>OverWritingBufferInt</TT> is full, the last item previously put into
     * the buffer will be overwritten.
     *
     * @param value the int to put into the OverWritingBufferInt
     */
    @Override
    public void put(int value) {
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
     * Returns the current state of the <TT>OverWritingBufferInt</TT>.
     *
     * @return the current state of the <TT>OverWritingBufferInt</TT>
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
     * Returns a new (and <TT>EMPTY</TT>) <TT>OverWritingBufferInt</TT> with the
     * same creation parameters as this one.
     * <P>
     * <I>Note: Only the size and structure of the
     * </I><TT>OverWritingBufferInt</TT><I> is cloned, not any stored data.</I>
     *
     * @return the cloned instance of this <TT>OverWritingBufferInt</TT>.
     */
    @Override
    public Object clone() {
        return new OverWritingBufferInt(buffer.length);
    }

    @Override
    public void removeAll() {
        counter = 0;
        firstIndex = 0;
        lastIndex = 0;
    }
}
