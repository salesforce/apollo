
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

import jcsp.lang.*;

    /**
 * Implements an <code>One2Any</code> channel that supports filtering at each end.
 *
 * @see One2AnyChannel
 * @see ReadFiltered
 * @see WriteFiltered
 *
 * @author Quickstone Technologies Limited
 */
class FilteredOne2AnyChannelImpl implements FilteredOne2AnyChannel
{
    /**
     * The filtered input end of the channel.
     */
    private FilteredSharedChannelInput in;

    /**
     * The filtered output end of the channel.
     */
    private FilteredChannelOutput out;

    /**
     * Constructs a new filtered channel from an existing channel.
     *
     * @param chan the existing channel.
     */
    public FilteredOne2AnyChannelImpl(One2AnyChannel chan)
    {
        in = new FilteredSharedChannelInputWrapper(chan.in());
        out = new FilteredChannelOutputWrapper(chan.out());
    }

    public SharedChannelInput in()
    {
        return in;
    }

    public ChannelOutput out()
    {
        return out;
    }

    public ReadFiltered inFilter()
    {
        return in;
    }

    public WriteFiltered outFilter()
    {
        return out;
    }
}
