
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
 * This defines the interface for writing to object channels.
 * <p>
 * A <i>writing-end</i>, conforming to this interface, is obtained from a
 * channel by invoking its <tt>out()</tt> method.
 * <H2>Description</H2> <TT>ChannelOutput</TT> defines the interface for writing
 * to object channels. The interface contains only one method -
 * <TT>write(Object o)</TT>. This method will block the calling process until
 * the <TT>Object</TT> has been accepted by the channel. In the (default) case
 * of a zero-buffered synchronising CSP channel, this happens only when a
 * process at the other end of the channel invokes (or has already invoked) a
 * <TT>read()</TT>.
 * <P>
 * <TT>ChannelOutput</TT> variables are used to hold channels that are going to
 * be used only for <I>output</I> by the declaring process. This is a security
 * matter -- by declaring a <TT>ChannelOutput</TT> interface, any attempt to
 * <I>input</I> from the channel will generate a compile-time error. For
 * example, the following code fragment will not compile:
 *
 * <PRE>
 * Object doRead(ChannelOutput c) {
 *     return c.read(); // illegal
 * }
 * </PRE>
 *
 * When configuring a <TT>CSProcess</TT> with output channels, they should be
 * declared as <TT>ChannelOutput</TT> variables. The actual channel passed, of
 * course, may belong to <I>any</I> channel class that implements
 * <TT>ChannelOutput</TT>.
 * <P>
 * Instances of any class may be written to a channel.
 *
 * <H2>Example</H2>
 * 
 * <PRE>
 * void doWrite(ChannelOutput c, Object o) {
 *     c.write(o);
 * }
 * </PRE>
 *
 * @see jcsp.lang.SharedChannelOutput
 * @see ChannelInput
 * @author P.D. Austin
 */

public interface ChannelOutput<T> extends Poisonable {
    /**
     * Write an Object to the channel.
     *
     * @param object the object to write to the channel
     */
    public void write(T object);
}
