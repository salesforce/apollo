
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
 * Implements an <code>AltingChannelInput</code> channel end that also supports read filters.
 *
 * @see AltingChannelInput
 * @see ReadFiltered
 *
 * @author Quickstone Technologies Limited
 */
public class FilteredAltingChannelInput
        extends AltingChannelInputWrapper
        implements FilteredChannelInput
{
    /**
     * Holds the filters installed for the read end of this channel.
     */
    private FilterHolder filters = null;

    /**
     * Constructs a new channel end that supports filtering by wrapping up an existing channel end.
     *
     * @param altingChannelInput the existing channel end.
     */
    FilteredAltingChannelInput(AltingChannelInput altingChannelInput)
    {
        super(altingChannelInput);
    }

    public Object read()
    {
        Object toFilter = super.read();
        for (int i = 0; filters != null && i < filters.getFilterCount(); i++)
            toFilter = filters.getFilter(i).filter(toFilter);
        return toFilter;
    }

    public void addReadFilter(Filter filter)
    {
        if (filters == null)
            filters = new FilterHolder();
        filters.addFilter(filter);
    }

    public void addReadFilter(Filter filter, int index)
    {
        if (filters == null)
            filters = new FilterHolder();
        filters.addFilter(filter, index);
    }

    public void removeReadFilter(Filter filter)
    {
        if (filters == null)
            filters = new FilterHolder();
        filters.removeFilter(filter);
    }

    public void removeReadFilter(int index)
    {
        if (filters == null)
            filters = new FilterHolder();
        filters.removeFilter(index);
    }

    public Filter getReadFilter(int index)
    {
        if (filters == null)
            filters = new FilterHolder();
        return filters.getFilter(index);
    }

    public int getReadFilterCount()
    {
        if (filters == null)
            return 0;
        return filters.getFilterCount();
    }
}
