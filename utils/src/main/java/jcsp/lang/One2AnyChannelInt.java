
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
 * This defines the interface for a <i>one-to-any</i> integer channel,
 * safe for use by one writer and many readers.
 * <P>
 * The only methods provided are to obtain the <i>ends</i> of the channel,
 * through which all reading and writing operations are done.
 * Only an appropriate <i>channel-end</i> should be plugged into a process
 * &ndash; not the <i>whole</i> channel.
 * A process may use its external channels in one direction only
 * &ndash; either for <i>writing</i> or <i>reading</i>.
 * </P>
 * <P>Actual channels conforming to this interface are made using the relevant
 * <tt>static</tt> construction methods from {@link Channel}.
 * Channels may be {@link Channel#one2anyInt() <i>synchronising</i>},
 * {@link Channel#one2anyInt(jcsp.util.ints.ChannelDataStoreInt) <i>buffered</i>},
 * {@link Channel#one2anyInt(int) <i>poisonable</i>}
 * or {@link Channel#one2anyInt(jcsp.util.ints.ChannelDataStoreInt,int) <i>both</i>}
 * <i>(i.e. buffered and poisonable)</i>.
 * </P>
 * <H2>Description</H2>
 * <TT>One2AnyChannelInt</TT> is an interface for a channel which is safe
 * for use by many reading processes but only one writer.  Reading processes
 * compete with each other to use the channel.  Only one reader and the writer will
 * actually be using the channel at any one time.  This is managed by the
 * channel &ndash; user processes just read from or write to it.
 * <P>
 * <I>Please note that this is a safely shared channel and not
 * a broadcaster.  Currently, broadcasting has to be managed by
 * writing an active process (see {@link jcsp.plugNplay.DynamicDelta}
 * for an example).</I>
 * </P>
 * <P>
 * All reading processes and the writing process commit to the channel
 * (i.e. may not back off).  This means that the reading processes
 * <I>may not</I> {@link Alternative <TT>ALT</TT>} on this channel.
 * </P>
 * <P>
 * The default semantics of the channel is that of CSP &ndash; i.e. it is
 * zero-buffered and fully synchronised.  A reading process must wait
 * for the matching writer and vice-versa.
 * </P>
 * <P>
 * The <tt>static</tt> methods of {@link Channel} construct channels with
 * either the default semantics or with buffering to user-specified capacity
 * and a range of blocking/overwriting policies.
 * Various buffering plugins are given in the <TT>jcsp.util</TT> package, but
 * <I>careful users</I> may write their own.
 * </P>
 * <P>
 * The {@link Channel} methods also provide for the construction of
 * {@link Poisonable} channels and for arrays of channels.
 *
 * <H3><A NAME="Caution">Implementation Note and Caution</H3>
 * <I>Fair</I> servicing of readers to this channel depends on the <I>fair</I>
 * servicing of requests to enter a <TT>synchronized</TT> block (or method) by
 * the underlying Java Virtual Machine (JVM).  Java does not specify how threads
 * waiting to synchronize should be handled.  Currently, Sun's standard JDKs queue
 * these requests - which is <I>fair</I>.  However, there is at least one JVM
 * that puts such competing requests on a stack - which is legal but <I>unfair</I>
 * and can lead to infinite starvation.  This is a problem for <I>any</I> Java system
 * relying on good behaviour from <TT>synchronized</TT>, not just for these
 * <I>1-any</I> channels.
 *
 * @see Channel
 * @see jcsp.lang.One2OneChannelInt
 * @see Any2OneChannelInt
 * @see Any2AnyChannelInt
 * @see jcsp.util.ints.ChannelDataStoreInt
 *
 * @author P.D. Austin and P.H. Welch
 */
public interface One2AnyChannelInt
{
    /**
     * Returns the input end of the channel.
     */
    public SharedChannelInputInt in();

    /**
     * Returns the output end of the channel.
     */
    public ChannelOutputInt out();
}
