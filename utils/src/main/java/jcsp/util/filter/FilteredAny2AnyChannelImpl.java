
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
import jcsp.lang.SharedChannelInput;
import jcsp.lang.SharedChannelOutput;

/**
 * This wraps up an Any2AnyChannel object so that its input and output ends are
 * separate objects. Both ends of the channel have filtering enabled.
 *
 * @author Quickstone Technologies Limited
 */
class FilteredAny2AnyChannelImpl<In, Out> implements FilteredAny2AnyChannel<In, Out> {
    /**
     * The input end of the channel.
     */
    private FilteredSharedChannelInput<In> in;

    /**
     * The output end of the channel.
     */
    private FilteredSharedChannelOutput<Out> out;

    /**
     * Constructs a new filtered channel object based on an existing channel.
     */
    FilteredAny2AnyChannelImpl(Any2AnyChannel<In, Out> chan) {
        in = new FilteredSharedChannelInputWrapper<In>(chan.in());
        out = new FilteredSharedChannelOutputWrapper<Out>(chan.out());
    }

    @Override
    public SharedChannelInput<In> in() {
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
