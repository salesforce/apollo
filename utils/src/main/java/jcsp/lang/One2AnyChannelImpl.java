
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

/**
 * This implements a one-to-any object channel, safe for use by one writer and
 * many readers.
 * <H2>Description</H2> <TT>One2AnyChannel</TT> is an implementation of a
 * channel which is safe for use by many reading processes but only one writer.
 * Reading processes compete with each other to use the channel. Only one reader
 * and the writer will actually be using the channel at any one time. This is
 * taken care of by <TT>One2AnyChannel</TT> -- user processes just read from or
 * write to it.
 * <P>
 * <I>Please note that this is a safely shared channel and not a broadcaster.
 * Currently, broadcasting has to be managed by writing active processes (see
 * {@link jcsp.plugNplay.DynamicDelta} for an example).</I>
 * <P>
 * All reading processes and the writing process commit to the channel (i.e. may
 * not back off). This means that the reading processes <I>may not</I>
 * {@link Alternative <TT>ALT</TT>} on this channel.
 * <P>
 * The default semantics of the channel is that of CSP -- i.e. it is
 * zero-buffered and fully synchronised. A reading process must wait for the
 * matching writer and vice-versa.
 * <P>
 * A factory pattern is used to create channel instances. The <tt>create</tt>
 * methods of {@link Channel} allow creation of channels, arrays of channels and
 * channels with varying semantics such as buffering with a user-defined
 * capacity or overwriting with various policies. Standard examples are given in
 * the <TT>jcsp.util</TT> package, but <I>careful users</I> may write their own.
 *
 * <H3><A NAME="Caution">Implementation Note and Caution</H3> <I>Fair</I>
 * servicing of readers to this channel depends on the <I>fair</I> servicing of
 * requests to enter a <TT>synchronized</TT> block (or method) by the underlying
 * Java Virtual Machine (JVM). Java does not specify how threads waiting to
 * synchronize should be handled. Currently, Sun's standard JDKs queue these
 * requests - which is <I>fair</I>. However, there is at least one JVM that puts
 * such competing requests on a stack - which is legal but <I>unfair</I> and can
 * lead to infinite starvation. This is a problem for <I>any</I> Java system
 * relying on good behaviour from <TT>synchronized</TT>, not just for these
 * <I>1-any</I> channels.
 *
 * @see One2OneChannel
 * @see Any2OneChannel
 * @see Any2AnyChannel
 * @see jcsp.util.ChannelDataStore
 *
 * @author P.D. Austin and P.H. Welch
 */

class One2AnyChannelImpl<T> extends One2AnyImpl<T> {
    One2AnyChannelImpl() {
        super(new One2OneChannelImpl<T>());
    }
}
