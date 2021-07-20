
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
 * Defines an interface for a factory that can create channels carrying
 * integers.
 *
 * @author Quickstone Technologies Limited
 * 
 * @deprecated These channel factories are deprecated in favour of the new
 *             one2one() methods in the Channel class.
 */
@Deprecated
public interface ChannelIntFactory {
    /**
     * Creates a new <code>One2One</code> channel.
     *
     * @return the created channel.
     */
    public One2OneChannelInt createOne2One();

    /**
     * Creates a new <code>Any2One</code> channel.
     *
     * @return the created channel.
     */
    public Any2OneChannelInt createAny2One();

    /**
     * Creates a new <code>One2Any</code> channel.
     *
     * @return the created channel.
     */
    public One2AnyChannelInt createOne2Any();

    /**
     * Creates a new <code>Any2Any</code> channel.
     *
     * @return the created channel.
     */
    public Any2AnyChannelInt createAny2Any();
}
