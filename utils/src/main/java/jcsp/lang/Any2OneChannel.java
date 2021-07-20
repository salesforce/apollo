
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
 * This defines an interface for an <i>any-to-one</i> Object channel, safe for
 * use by many writers and one reader.
 * <P>
 * The only methods provided are to obtain the <i>ends</i> of the channel,
 * through which all reading and writing operations are done. Only an
 * appropriate <i>channel-end</i> should be plugged into a process &ndash; not
 * the <i>whole</i> channel. A process may use its external channels in one
 * direction only &ndash; either for <i>writing</i> or <i>reading</i>.
 * </P>
 * <P>
 * Actual channels conforming to this interface are made using the relevant
 * <tt>static</tt> construction methods from {@link Channel}. Channels may be
 * {@link Channel#any2one() <i>synchronising</i>},
 * {@link Channel#any2one(jcsp.util.ChannelDataStore) <i>buffered</i>},
 * {@link Channel#any2one(int) <i>poisonable</i>} or
 * {@link Channel#any2one(jcsp.util.ChannelDataStore,int) <i>both</i>} <i>(i.e.
 * buffered and poisonable)</i>.
 * </P>
 * <H2>Description</H2> <TT>Any2OneChannel</TT> is an interface for a channel
 * which is safe for use by many writing processes but only one reader. Writing
 * processes compete with each other to use the channel. Only the reader and one
 * writer will actually be using the channel at any one time. This is managed by
 * the channel &ndash; user processes just read from or write to it.
 * </P>
 * <P>
 * <I>Please note that this is a safely shared channel and not a message
 * gatherer. Currently, gathering has to be managed by writing an active
 * process.</I>
 * <P>
 * The reading process may {@link Alternative <TT>ALT</TT>} on this channel. The
 * writing process is committed (i.e. it may not back off).
 * </P>
 * <P>
 * The default semantics of the channel is that of CSP &ndash; i.e. it is
 * zero-buffered and fully synchronised. The reading process must wait for a
 * matching writer and vice-versa.
 * </P>
 * <P>
 * The <tt>static</tt> methods of {@link Channel} construct channels with either
 * the default semantics or with buffering to user-specified capacity and a
 * range of blocking/overwriting policies. Various buffering plugins are given
 * in the <TT>jcsp.util</TT> package, but <I>careful users</I> may write their
 * own.
 * </P>
 * <P>
 * The {@link Channel} methods also provide for the construction of
 * {@link Poisonable} channels and for arrays of channels.
 *
 * <H3><A NAME="Caution">Implementation Note and Caution</H3> <I>Fair</I>
 * servicing of writers to this channel depends on the <I>fair</I> servicing of
 * requests to enter a <TT>synchronized</TT> block (or method) by the underlying
 * Java Virtual Machine (JVM). Java does not specify how threads waiting to
 * synchronize should be handled. Currently, Sun's standard JDKs queue these
 * requests - which is <I>fair</I>. However, there is at least one JVM that puts
 * such competing requests on a stack - which is legal but <I>unfair</I> and can
 * lead to infinite starvation. This is a problem for <I>any</I> Java system
 * relying on good behaviour from <TT>synchronized</TT>, not just for these
 * <I>any-1</I> channels.
 *
 * @see Channel
 * @see Alternative
 * @see jcsp.lang.One2OneChannel
 * @see jcsp.lang.One2AnyChannel
 * @see Any2AnyChannel
 * @see jcsp.util.ChannelDataStore
 *
 * @author P.D. Austin and P.H. Welch
 */
public interface Any2OneChannel<In, Out> {
    /**
     * Returns the input end of the channel.
     */
    public AltingChannelInput<In> in();

    /**
     * Returns the output end of the channel.
     */
    public SharedChannelOutput<Out> out();
}
