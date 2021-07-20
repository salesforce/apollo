
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
import jcsp.lang.ChannelInput;
import jcsp.lang.ChannelOutput;
import jcsp.lang.SharedChannelInput;
import jcsp.lang.SharedChannelOutput;

/**
 * Static factory for creating channel end wrappers that support filtering.
 *
 * @author Quickstone Technologies Limited
 */
public class FilteredChannelEnd {
    /**
     * The default factory for creating the channel ends.
     */
    private static final FilteredChannelEndFactory factory = new FilteredChannelEndFactory();

    /**
     * Private constructor to prevent any instances of this static factory from
     * being created.
     */
    private FilteredChannelEnd() {
        // Noone's creating one of these
    }

    /**
     * Creates a new filtered input channel end around an existing input channel
     * end. The channel end can be used as a guard in an <code>Alternative</code>.
     *
     * @param in the existing channel end to create a filtered form of.
     * @return the new channel end with filtering ability.
     */
    public static FilteredAltingChannelInput createFiltered(AltingChannelInput in) {
        return factory.createFiltered(in);
    }

    /**
     * Creates a new filtered input channel end around an existing input channel
     * end.
     *
     * @param in the existing channel end to create a filtered form of.
     * @return the new channel end with filtering ability.
     */
    public static FilteredChannelInput createFiltered(ChannelInput in) {
        return factory.createFiltered(in);
    }

    /**
     * Creates a new filtered input channel end around an existing input channel end
     * that can be shared by multiple processes.
     *
     * @param in the existing channel end to create a filtered form of,
     * @return the new channel end with filtering ability.
     */
    public static FilteredSharedChannelInput createFiltered(SharedChannelInput in) {
        return factory.createFiltered(in);
    }

    /**
     * Creates a new filtered output channel end around an existing output channel
     * end.
     *
     * @param out the existing channel end to create a filtered form of.
     */
    public static FilteredChannelOutput createFiltered(ChannelOutput out) {
        return factory.createFiltered(out);
    }

    /**
     * Creates a new filtered output channel end around an existing output channel
     * end that can be shared by multiple processes.
     *
     * @param out the existing channel end to create a filtered form of.
     * @return the new channel end with filtering ability.
     */
    public static FilteredSharedChannelOutput createFiltered(SharedChannelOutput out) {
        return factory.createFiltered(out);
    }
}
