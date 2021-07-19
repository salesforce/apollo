
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
 * This wraps up an Any2AnyChannel object so that its
 * input and output ends are separate objects. Both ends of the channel
 * have filtering enabled.
 *
 * @author Quickstone Technologies Limited
 */
class FilteredAny2AnyChannelImpl implements FilteredAny2AnyChannel
{
    /**
     * The input end of the channel.
     */
    private FilteredSharedChannelInput in;

    /**
     * The output end of the channel.
     */
    private FilteredSharedChannelOutput out;

    /**
     * Constructs a new filtered channel object based on an existing channel.
     */
    FilteredAny2AnyChannelImpl(Any2AnyChannel chan)
    {
        in = new FilteredSharedChannelInputWrapper(chan.in());
        out = new FilteredSharedChannelOutputWrapper(chan.out());
    }

    public SharedChannelInput in()
    {
        return in;
    }

    public SharedChannelOutput out()
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
