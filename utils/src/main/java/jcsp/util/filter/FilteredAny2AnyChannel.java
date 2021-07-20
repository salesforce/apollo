
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

package jcsp.util.filter;

import jcsp.lang.Any2AnyChannel;

/**
 * Interface for an Any2Any channel that has support for filtering at both ends.
 *
 * @see Any2AnyChannel
 * @see ReadFiltered
 * @see WriteFiltered
 *
 * @author Quickstone Technologies Limited
 */
public interface FilteredAny2AnyChannel<In, Out> extends Any2AnyChannel<In, Out> {
    /**
     * Returns an interface for configuring read filters on the channel.
     */
    public ReadFiltered inFilter();

    /**
     * Returns an interface for configuring write filters on the channel.
     */
    public WriteFiltered outFilter();

}
