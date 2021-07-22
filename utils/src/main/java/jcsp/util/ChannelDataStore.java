
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

/**
 * This is the interface for object channel plug-ins that define their buffering
 * characteristics.
 * <H2>Description</H2> <TT>ChannelDataStore</TT> defines the interface to
 * optional logic used by channels defined in the <TT>jcsp.lang</TT> package to
 * manage the data being communicated. Implementations are provided yielding a
 * range of buffering properties (e.g. {@link Buffer <i>fixed sized (block when
 * full)</i>}, {@link OverWriteOldestBuffer <i>fixed size (overwrite oldest data
 * when full)</i>}, {@link OverFlowingBuffer <i>fixed size (accept but discard
 * new data when full)</i>}, {@link InfiniteBuffer <i>infinitely
 * expandable</i>}).
 * <P>
 * Channels are constructed using the static construction methods of
 * {@link jcsp.lang.Channel}. By default, channels will be constructed with
 * standard CSP semantics &ndash; no buffering and full synchronisation between
 * reading and writing processes (e.g. {@link jcsp.lang.Channel#one2one()}). To
 * construct buffered channels, plug in the appropriate
 * <TT>ChannelDataStore</TT> (e.g.
 * {@link jcsp.lang.Channel#one2one(ChannelDataStore)}).
 * <P>
 * <I>Note: JCSP users should not normally need to define their own
 * implementations of this interface. However, implementors may assume that
 * </I><TT>ChannelDataStore</TT><I> methods are always invoked (by the various
 * channel classes within </I><TT>org.jcsp.lang</TT><I>) in a thread-safe way
 * &ndash; i.e. that there will be no race hazards between invocations of
 * {@link #get() <tt>get</tt>} and {@link #put(Object) <tt>put</tt>}). They may
 * also assume that the documented pre-conditions for invoking the
 * </I><TT>get</TT><I> and </I><TT>put</TT><I> methods will be met.
 * <TT>ChannelDataStore</TT> is only intended for defining the behaviour of
 * buffers &ndash; it is not intended for any other purpose (e.g. for
 * data-processing or filtering channels). </I>
 *
 * @see ZeroBuffer
 * @see Buffer
 * @see OverWriteOldestBuffer
 * @see jcsp.util.OverWritingBuffer
 * @see OverFlowingBuffer
 * @see InfiniteBuffer
 * @see jcsp.lang.Channel
 *
 * @author P.D. Austin
 */

public interface ChannelDataStore<T> extends Cloneable {
    /**
     * Indicates that the <TT>ChannelDataStore</TT> is empty -- it can accept only a
     * <TT>put</TT>.
     */
    public final static int EMPTY = 0;

    /**
     * Indicates that the <TT>ChannelDataStore</TT> is neither empty nor full -- it
     * can accept either a <TT>put</TT> or a <TT>get</TT> call.
     */
    public final static int NONEMPTYFULL = 1;

    /**
     * Indicates that the <TT>ChannelDataStore</TT> is full -- it can accept only a
     * <TT>get</TT>.
     */
    public final static int FULL = 2;

    /**
     * Returns the current state of the <TT>ChannelDataStore</TT>.
     *
     * @return the current state of the <TT>ChannelDataStore</TT> (<TT>EMPTY</TT>,
     *         <TT>NONEMPTYFULL</TT> or <TT>FULL</TT>)
     */
    public abstract int getState();

    /**
     * Puts a new <TT>Object</TT> into the <TT>ChannelDataStore</TT>.
     * <P>
     * <I>Pre-condition</I>: <TT>getState</TT> must not currently return
     * <TT>FULL</TT>.
     *
     * @param value the Object to put into the ChannelDataStore
     */
    public abstract void put(T value);

    /**
     * Returns an <TT>Object</TT> from the <TT>ChannelDataStore</TT>.
     * <P>
     * <I>Pre-condition</I>: <TT>getState</TT> must not currently return
     * <TT>EMPTY</TT>.
     *
     * @return an <TT>Object</TT> from the <TT>ChannelDataStore</TT>
     */
    public abstract T get();

    /**
     * Begins an extended read on the buffer, returning the data for the extended
     * read.
     * <p>
     * <I>Pre-condition</I>: <TT>getState</TT> must not currently return
     * <TT>EMPTY</TT>.
     * <p>
     * The exact behaviour of this method depends on your buffer. When a process
     * performs an extended rendezvous on a buffered channel, it will first call
     * this method, then the {@link #endGet} method.
     * <p>
     * A FIFO buffer would implement this method as returning the value from the
     * front of the buffer and the next call would remove the value. An overflowing
     * buffer would do the same.
     * <p>
     * However, for an overwriting buffer it is more complex. Refer to the
     * documentation for {@link OverWritingBuffer#startGet} and
     * {@link OverWriteOldestBuffer#startGet} for details
     * 
     * @return The object to be read from the channel at the beginning of the
     *         extended rendezvous
     *
     * @see #endGet
     */
    public abstract T startGet();

    /**
     * Ends an extended read on the buffer.
     * 
     * The channels guarantee that this method will be called exactly once after
     * each {@link #startGet <code>startGet</code>} call. During the period between
     * {@link #startGet <code>startGet</code>} and {@link #endGet
     * <code>endGet</code>}, it is possible that {@link #put <code>put</code>} will
     * be called, but not {@link #get <code>get</code>}.
     *
     * @see #startGet
     */
    public abstract void endGet();

    /**
     * Returns a new (and <TT>EMPTY</TT>) <TT>ChannelDataStore</TT> with the same
     * creation parameters as this one.
     * <P>
     * <I>Note: Only the size and structure of the </I><TT>ChannelDataStore</TT><I>
     * should be cloned, not any stored data.</I>
     *
     * @return the cloned instance of this <TT>ChannelDataStore</TT>.
     */
    public abstract ChannelDataStore<T> clone();

    /**
     * Deletes all items in the buffer, leaving it empty.
     *
     */

    public abstract void removeAll();
}
