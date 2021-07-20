
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

import jcsp.util.ints.ChannelDataStoreInt;

/**
 * This implements an any-to-any integer channel with user-definable buffering,
 * safe for use by many writers and many readers.
 * <H2>Description</H2> <TT>BufferedAny2AnyChannelIntImpl</TT> implements an
 * any-to-any integer channel with user-definable buffering. It is safe for use
 * by any number of reading or writing processes. Reading processes compete with
 * each other to use the channel. Writing processes compete with each other to
 * use the channel. Only the reader and one writer will actually be using the
 * channel at any one time. This is taken care of by
 * <TT>BufferedAny2AnyChannelIntImpl</TT> -- user processes just read from or
 * write to it.
 * <P>
 * <I>Please note that this is a sefely shared channel and not a multicaster.
 * Currently, multicasting has to be managed by writing active processes (see
 * {@link jcsp.plugNplay.DynamicDelta} for an example of broadcasting).</I>
 * <P>
 * All reading processes and writing processes commit to the channel (i.e. may
 * not back off). This means that the reading processes <I>may not</I>
 * {@link Alternative <TT>ALT</TT>} on this channel.
 * <P>
 * The constructor requires the user to provide the channel with a
 * <I>plug-in</I> driver conforming to the {@link ChannelDataStoreInt
 * <TT>ChannelDataStoreInt</TT>} interface. This allows a variety of different
 * channel semantics to be introduced -- including buffered channels of
 * user-defined capacity (including infinite), overwriting channels (with
 * various overwriting policies) etc.. Standard examples are given in the
 * <TT>jcsp.util</TT> package, but <I>careful users</I> may write their own.
 *
 * <H3><A NAME="Caution">Implementation Note and Caution</H3> <I>Fair</I>
 * servicing of readers and writers to this channel depends on the <I>fair</I>
 * servicing of requests to enter a <TT>synchronized</TT> block (or method) by
 * the underlying Java Virtual Machine (JVM). Java does not specify how threads
 * waiting to synchronize should be handled. Currently, Sun's standard JDKs
 * queue these requests - which is <I>fair</I>. However, there is at least one
 * JVM that puts such competing requests on a stack - which is legal but
 * <I>unfair</I> and can lead to infinite starvation. This is a problem for
 * <I>any</I> Java system relying on good behaviour from <TT>synchronized</TT>,
 * not just for these <I>any-any</I> channels.
 *
 * @see BufferedOne2OneChannel
 * @see BufferedOne2AnyChannel
 * @see BufferedAny2AnyChannel
 * @see ChannelDataStoreInt
 *
 * @author P.D. Austin
 * @author P.H. Welch
 */

class BufferedAny2AnyChannelIntImpl extends Any2AnyIntImpl {
    public BufferedAny2AnyChannelIntImpl(ChannelDataStoreInt data) {
        super(new BufferedOne2OneChannelIntImpl(data));
    }
}
