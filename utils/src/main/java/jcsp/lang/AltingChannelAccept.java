
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
 * This extends {@link Guard} and {@link ChannelAccept} to enable a process to
 * choose between many CALL channel (and other) events.
 * <H2>Description</H2> <TT>AltingChannelAccept</TT> extends {@link Guard} and
 * {@link ChannelAccept} to enable a process to choose between many CALL channel
 * (and other) events. The methods inherited from <TT>Guard</TT> are of no
 * concern to users of this package.
 *
 * <H2>Example</H2> See the explanations and examples documented in
 * {@link One2OneCallChannel} and {@link Any2OneCallChannel}.
 *
 * @see Alternative
 * @see One2OneCallChannel
 * @see Any2OneCallChannel
 *
 * @author P.H. Welch
 */

public abstract class AltingChannelAccept extends Guard implements ChannelAccept {
    // nothing to add ...
}
