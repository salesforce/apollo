
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
 * Wrapper for an output channel end to include write filtering ability.
 *
 * @author Quickstone Technologies Limited
 */
class FilteredChannelOutputWrapper
        extends ChannelOutputWrapper
        implements FilteredChannelOutput
{
    /**
     * Set of write filters installed.
     */
    private FilterHolder filters = null;

    /**
     * Constructs a new <code>FilteredChannelOutputWrapper</code> around the given output channel end.
     *
     * @param out the existing output channel.
     */
    public FilteredChannelOutputWrapper(ChannelOutput out)
    {
        super(out);
    }

    public void write(Object data)
    {
        for (int i = 0; filters != null && i < filters.getFilterCount(); i++)
            data = filters.getFilter(i).filter(data);
        super.write(data);
    }

    public void addWriteFilter(Filter filter)
    {
        if (filters == null)
            filters = new FilterHolder();
        filters.addFilter(filter);
    }

    public void addWriteFilter(Filter filter, int index)
    {
        if (filters == null)
            filters = new FilterHolder();
        filters.addFilter(filter, index);
    }

    public void removeWriteFilter(Filter filter)
    {
        if (filters == null)
            filters = new FilterHolder();
        filters.removeFilter(filter);
    }

    public void removeWriteFilter(int index)
    {
        if (filters == null)
            filters = new FilterHolder();
        filters.removeFilter(index);
    }

    public Filter getWriteFilter(int index)
    {
        if (filters == null)
            filters = new FilterHolder();
        return filters.getFilter(index);
    }

    public int getWriteFilterCount()
    {
        if (filters == null)
            return 0;
        return filters.getFilterCount();
    }
}
