
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
 * <p>
 * Factory for creating filtered channel ends around existing channel ends.
 * </p>
 *
 * <p>
 * An instance of this class can be created and used, or alternatively the
 * static factory <code>FilteredChannelEnd</code> may be more convenient.
 * </p>
 *
 * @author Quickstone Technologies Limited
 */
public class FilteredChannelEndFactory {
    /**
     * Constructs a new <code>FilteredChannelEndFactory</code>.
     */
    public FilteredChannelEndFactory() {
        super();
    }

    /**
     * Creates a new filtered channel input end around an existing channel end. The
     * created channel end can be used as a guard in an <code>Alternative</code>.
     *
     * @param in the existing channel end.
     * @return the created channel end.
     */
    public FilteredAltingChannelInput createFiltered(AltingChannelInput in) {
        return new FilteredAltingChannelInput(in);
    }

    /**
     * Creates a new filtered channel input end around an existing channel end.
     *
     * @param in the existing channel end.
     * @return the created channel end.
     */
    public FilteredChannelInput createFiltered(ChannelInput in) {
        return new FilteredChannelInputWrapper(in);
    }

    /**
     * Creates a new filtered channel input end around an existing channel end. The
     * created channel end can be shared by multiple processes.
     *
     * @param in the existing channel end.
     * @return the created channel end.
     */
    public FilteredSharedChannelInput createFiltered(SharedChannelInput in) {
        return new FilteredSharedChannelInputWrapper(in);
    }

    /**
     * Creates a new filtered channel output end around an existing channel end.
     *
     * @param out the existing channel end.
     * @return the created channel end.
     */
    public FilteredChannelOutput createFiltered(ChannelOutput out) {
        return new FilteredChannelOutputWrapper(out);
    }

    /**
     * Creates a new filtered channel output end around an existing channel end. The
     * created channel end can be shared by multiple processes.
     *
     * @param out the existing channel end.
     * @return the created channel end.
     */
    public FilteredSharedChannelOutput createFiltered(SharedChannelOutput out) {
        return new FilteredSharedChannelOutputWrapper(out);
    }
}
