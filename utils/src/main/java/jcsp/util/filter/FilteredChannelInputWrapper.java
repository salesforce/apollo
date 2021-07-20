
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

import jcsp.lang.ChannelInput;
import jcsp.lang.ChannelInputWrapper;

/**
 * Wrapper for an input channel end to include read filtering functionality.
 *
 * @author Quickstone Technologies Limited
 */
@SuppressWarnings("deprecation")
class FilteredChannelInputWrapper<T> extends ChannelInputWrapper<T> implements FilteredChannelInput<T> {
    /**
     * Set of read filters installed.
     */
    private FilterHolder filters = null;

    /**
     * Constructs a new <code>FilteredChannelInputWrapper</code> around the existing
     * channel end.
     *
     * @param in channel end to create the wrapper around.
     */
    FilteredChannelInputWrapper(ChannelInput<T> in) {
        super(in);
    }

    @Override
    @SuppressWarnings("unchecked")
    public T read() {
        T toFilter = super.read();
        for (int i = 0; filters != null && i < filters.getFilterCount(); i++)
            toFilter = (T) filters.getFilter(i).filter(toFilter);
        return toFilter;
    }

    @Override
    public void addReadFilter(Filter filter) {
        if (filters == null)
            filters = new FilterHolder();
        filters.addFilter(filter);
    }

    @Override
    public void addReadFilter(Filter filter, int index) {
        if (filters == null)
            filters = new FilterHolder();
        filters.addFilter(filter, index);
    }

    @Override
    public void removeReadFilter(Filter filter) {
        if (filters == null)
            filters = new FilterHolder();
        filters.removeFilter(filter);
    }

    @Override
    public void removeReadFilter(int index) {
        if (filters == null)
            filters = new FilterHolder();
        filters.removeFilter(index);
    }

    @Override
    public Filter getReadFilter(int index) {
        if (filters == null)
            filters = new FilterHolder();
        return filters.getFilter(index);
    }

    @Override
    public int getReadFilterCount() {
        if (filters == null)
            return 0;
        return filters.getFilterCount();
    }
}
