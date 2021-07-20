
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
 * overwriting its oldest data if full.
 * <H2>Description</H2> <TT>OverWriteOldestBufferInt</TT> is an implementation
 * of <TT>ChannelDataStoreInt</TT> that yields a <I>FIFO</I> buffered semantics
 * for a channel. When empty, the channel blocks readers. When full, a writer
 * will overwrite the <I>oldest</I> item left unread in the channel. See the
 * <tt>static</tt> construction methods of {@link jcsp.lang.Channel}
 * ({@link jcsp.lang.Channel#one2oneInt(ChannelDataStoreInt)} etc.).
 * <P>
 * The <TT>getState</TT> method returns <TT>EMPTY</TT> or <TT>NONEMPTYFULL</TT>,
 * but never <TT>FULL</TT>.
 *
 * @see ZeroBufferInt
 * @see BufferInt
 * @see OverWritingBufferInt
 * @see OverFlowingBufferInt
 * @see InfiniteBufferInt
 * @see jcsp.lang.ChannelInt
 *
 * @author P.D. Austin
 */
//}}}

public class OverWriteOldestBufferInt implements ChannelDataStoreInt, Serializable {
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
     * Construct a new <TT>OverWriteOldestBufferInt</TT> with the specified size.
     *
     * @param size the number of ints the OverWriteOldestBufferInt can store.
     * @throws BufferIntSizeError if <TT>size</TT> is zero or negative. Note: no
     *                            action should be taken to
     *                            <TT>try</TT>/<TT>catch</TT> this exception -
     *                            application code generating it is in error and
     *                            needs correcting.
     */
    public OverWriteOldestBufferInt(int size) {
        if (size <= 0)
            throw new BufferIntSizeError("\n*** Attempt to create an overwriting buffered channel with negative or zero capacity");
        buffer = new int[size];
    }

    /**
     * Returns the oldest <TT>int</TT> from the <TT>OverWriteOldestBufferInt</TT>
     * and removes it.
     * <P>
     * <I>Pre-condition</I>: <TT>getState</TT> must not currently return
     * <TT>EMPTY</TT>.
     *
     * @return the oldest <TT>int</TT> from the <TT>OverWriteOldestBufferInt</TT>
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
     * The semantics of an extended rendezvous on an overwrite-oldest buffer are
     * slightly complicated, but hopefully intuitive.
     * 
     * When a reader begins an extended rendezvous, the oldest value is returned
     * from the buffer (as it would be for a call to {@link #get()}). While an
     * extended rendezvous is ongoing, the writer may (repeatedly) write to the
     * buffer, without ever blocking.
     * 
     * When the reader finishes an extended rendezvous, the following options are
     * possible:
     * <ul>
     * <li>The writer has not written to the channel during the rendezvous. In this
     * case, the value that was read at the start of the rendezvous is removed from
     * the buffer.</li>
     * <li>The writer has written to the channel during the rendezvous, but has not
     * over-written the value that was read at the start of the rendezvous. In this
     * case, the value that was read at the start of the rendezvous is removed from
     * the buffer.</li>
     * <li>The writer has written to the channel during the rendezvous, and has
     * over-written (perhaps repeatedly) the value that was read at the start of the
     * rendezvous. In this case, the value that was read at the start of the
     * rendezvous is no longer in the buffer, and hence nothing is removed.</li>
     * </ul>
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
        if (!valueWrittenWhileFull) {
            // Our data hasn't been over-written so remove it:
            firstIndex = (firstIndex + 1) % buffer.length;
            counter--;
        }
    }

    /**
     * Puts a new <TT>int</TT> into the <TT>OverWriteOldestBufferInt</TT>.
     * <P>
     * If <TT>OverWriteOldestBufferInt</TT> is full, the <I>oldest</I> item left
     * unread in the buffer will be overwritten.
     *
     * @param value the int to put into the OverWriteOldestBufferInt
     */
    @Override
    public void put(int value) {
        if (counter == buffer.length) {
            firstIndex = (firstIndex + 1) % buffer.length;
            valueWrittenWhileFull = true;
        } else {
            counter++;
        }
        buffer[lastIndex] = value;
        lastIndex = (lastIndex + 1) % buffer.length;
    }

    /**
     * Returns the current state of the <TT>OverWriteOldestBufferInt</TT>.
     *
     * @return the current state of the <TT>OverWriteOldestBufferInt</TT>
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
     * Returns a new (and <TT>EMPTY</TT>) <TT>OverWriteOldestBufferInt</TT> with the
     * same creation parameters as this one.
     * <P>
     * <I>Note: Only the size and structure of the
     * </I><TT>OverWriteOldestBufferInt</TT><I> is cloned, not any stored data.</I>
     *
     * @return the cloned instance of this <TT>OverWriteOldestBufferInt</TT>.
     */
    @Override
    public Object clone() {
        return new OverWriteOldestBufferInt(buffer.length);
    }

    @Override
    public void removeAll() {
        counter = 0;
        firstIndex = 0;
        lastIndex = 0;
    }
}
