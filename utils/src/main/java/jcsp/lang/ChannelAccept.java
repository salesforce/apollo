
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
 * This defines the interface for accepting CALL channels.
 * <H2>Description</H2> <TT>ChannelAccept</TT> defines the interface for
 * accepting CALL channels. The interface contains only one method -
 * {@link #accept <TT>accept</TT>}.
 *
 * <H2>Example</H2> See the explanations and examples documented in the CALL
 * channel super-classes (listed below).
 *
 * @see jcsp.lang.One2OneCallChannel
 * @see jcsp.lang.Any2OneCallChannel
 * @see jcsp.lang.One2AnyCallChannel
 * @see jcsp.lang.Any2AnyCallChannel
 *
 * @author P.H. Welch
 */

public interface ChannelAccept {
    /**
     * This is invoked by a <I>server</I> when it commits to accepting a CALL from a
     * <I>client</I>. The parameter supplied must be a reference to this
     * <I>server</I> - see the <A HREF="One2OneCallChannel.html#Accept">example</A>
     * from {@link One2OneCallChannel}. It will not complete until a CALL has been
     * made. If the derived CALL channel has set the <TT>selected</TT> field in the
     * way defined by the standard
     * <A HREF="One2OneCallChannel.html#One2OneFooChannel">calling sequence</A>, the
     * value returned by this method will indicate which method was called.
     *
     * @param server the <I>server</I> process receiving the CALL.
     */
    public int accept(CSProcess server);
}
