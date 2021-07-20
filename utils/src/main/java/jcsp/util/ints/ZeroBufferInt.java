
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
 * This is used to create a zero-buffered integer channel that never loses data.
 * <H2>Description</H2> <TT>ZeroBufferInt</TT> is an implementation of
 * <TT>ChannelDataStoreInt</TT> that yields the standard <I><B>CSP</B></I>
 * semantics for a channel -- that is zero buffered with direct synchronisation
 * between reader and writer. Unless specified otherwise, this is the default
 * behaviour for channels. See the <tt>static</tt> construction methods of
 * {@link jcsp.lang.Channel}
 * ({@link jcsp.lang.Channel#one2oneInt(jcsp.util.ints.ChannelDataStoreInt)}
 * etc.).
 * <P>
 * The <TT>getState</TT> method will return <TT>FULL</TT> if there is an output
 * waiting on the channel and <TT>EMPTY</TT> if there is not.
 *
 * @see BufferInt
 * @see jcsp.util.ints.OverWriteOldestBufferInt
 * @see jcsp.util.ints.OverWritingBufferInt
 * @see jcsp.util.ints.OverFlowingBufferInt
 * @see jcsp.util.ints.InfiniteBufferInt
 * @see jcsp.lang.ChannelInt
 *
 * @author P.D. Austin
 */
//}}}

public class ZeroBufferInt implements ChannelDataStoreInt, Serializable {
    /** The current state */
    private int state = EMPTY;

    /** The int */
    private int value;

    /**
     * Returns the <TT>int</TT> from the <TT>ZeroBufferInt</TT>.
     * <P>
     * <I>Pre-condition</I>: <TT>getState</TT> must not currently return
     * <TT>EMPTY</TT>.
     *
     * @return the <TT>int</TT> from the <TT>ZeroBufferInt</TT>
     */
    @Override
    public int get() {
        state = EMPTY;
        int o = value;
        return o;
    }

    /**
     * Begins an extended rendezvous - simply returns the next integer in the
     * buffer. This function does not remove the integer.
     * 
     * <I>Pre-condition</I>: <TT>getState</TT> must not currently return
     * <TT>EMPTY</TT>.
     * 
     * @return The integer in the buffer.
     */
    @Override
    public int startGet() {
        return value;
    }

    /**
     * Ends the extended rendezvous by clearing the buffer.
     */
    @Override
    public void endGet() {
        state = EMPTY;
    }

    /**
     * Puts a new <TT>int</TT> into the <TT>ZeroBufferInt</TT>.
     * <P>
     * <I>Pre-condition</I>: <TT>getState</TT> must not currently return
     * <TT>FULL</TT>.
     *
     * @param value the int to put into the ZeroBufferInt
     */
    @Override
    public void put(int value) {
        state = FULL;
        this.value = value;
    }

    /**
     * Returns the current state of the <TT>ZeroBufferInt</TT>.
     *
     * @return the current state of the <TT>ZeroBufferInt</TT> (<TT>EMPTY</TT> or
     *         <TT>FULL</TT>)
     */
    @Override
    public int getState() {
        return state;
    }

    /**
     * Returns a new (and <TT>EMPTY</TT>) <TT>ZeroBufferInt</TT> with the same
     * creation parameters as this one.
     * <P>
     * <I>Note: Only the size and structure of the </I><TT>ZeroBufferInt</TT><I> is
     * cloned, not any stored data.</I>
     *
     * @return the cloned instance of this <TT>ZeroBufferInt</TT>.
     */
    @Override
    public Object clone() {
        return new ZeroBufferInt();
    }

    @Override
    public void removeAll() {
        state = EMPTY;
    }
}
