
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

/**
 * Interface for channel plug-ins that define filtering operations -
 * transformations on the data as it is read or written. A channel (or channel
 * end) that supports filtering will implement the <code>ReadFiltered</code> or
 * <code>WriteFiltered</code> interface which allows instances of
 * <code>Filter</code> to be installed or removed from the channel.
 *
 * @see jcsp.util.filter.FilteredChannel
 * @see jcsp.util.filter.FilteredChannelEnd
 * @see jcsp.util.filter.ReadFiltered
 * @see jcsp.util.filter.WriteFiltered
 *
 * @author Quickstone Technologies Limited
 */
public interface Filter {
    /**
     * Applies the filter operation. The object given can be modified and returned
     * or another object substituted in its place.
     *
     * @param obj the original object in the channel communication.
     * @return the modified/substituted object after filtration.
     */
    public Object filter(Object obj);
}
