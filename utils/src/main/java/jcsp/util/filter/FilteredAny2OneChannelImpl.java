
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

import jcsp.lang.AltingChannelInput;
import jcsp.lang.Any2OneChannel;
import jcsp.lang.SharedChannelOutput;

/**
 * This wraps up an Any2OneChannel object so that its input and output ends are
 * separate objects. Both ends of the channel have filtering enabled.
 *
 * @author Quickstone Technologies Limited
 */
class FilteredAny2OneChannelImpl<In, Out> implements FilteredAny2OneChannel<In, Out> {
    /**
     * The input end of the channel.
     */
    private FilteredAltingChannelInput<In> in;

    /**
     * The output end of the channel.
     */
    private FilteredSharedChannelOutput<Out> out;

    /**
     * Constructs a new filtered channel over the top of an existing channel.
     */
    public FilteredAny2OneChannelImpl(Any2OneChannel<In, Out> chan) {
        in = new FilteredAltingChannelInput<In>(chan.in());
        out = new FilteredSharedChannelOutputWrapper<Out>(chan.out());
    }

    @Override
    public AltingChannelInput<In> in() {
        return in;
    }

    @Override
    public SharedChannelOutput<Out> out() {
        return out;
    }

    @Override
    public ReadFiltered inFilter() {
        return in;
    }

    @Override
    public WriteFiltered outFilter() {
        return out;
    }
}
