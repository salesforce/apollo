
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

import jcsp.lang.SharedChannelOutput;

/**
 * This is wrapper for a <code>SharedChannelOutput</code> that adds write
 * filtering. Instances of this class can be safely used by multiple concurrent
 * processes.
 *
 * @author Quickstone Technologies Limited
 */
public class FilteredSharedChannelOutputWrapper<T> extends FilteredChannelOutputWrapper<T>
                                               implements FilteredSharedChannelOutput<T> {

    /**
     * The synchronization object to protect the writers from each other when they
     * read data or update the write filters.
     */
    private Object synchObject;

    /**
     * Constructs a new wrapper for the given channel output end.
     *
     * @param out the existing channel end.
     */
    public FilteredSharedChannelOutputWrapper(SharedChannelOutput<T> out) {
        super(out);
        synchObject = new Object();
    }

    @Override
    public void write(T data) {
        synchronized (synchObject) {
            super.write(data);
        }
    }

    @Override
    public void addWriteFilter(Filter filter) {
        synchronized (synchObject) {
            super.addWriteFilter(filter);
        }
    }

    @Override
    public void addWriteFilter(Filter filter, int index) {
        synchronized (synchObject) {
            super.addWriteFilter(filter, index);
        }
    }

    @Override
    public void removeWriteFilter(Filter filter) {
        synchronized (synchObject) {
            super.removeWriteFilter(filter);
        }
    }

    @Override
    public void removeWriteFilter(int index) {
        synchronized (synchObject) {
            super.removeWriteFilter(index);
        }
    }

    @Override
    public Filter getWriteFilter(int index) {
        synchronized (synchObject) {
            return super.getWriteFilter(index);
        }
    }

    @Override
    public int getWriteFilterCount() {
        synchronized (synchObject) {
            return super.getWriteFilterCount();
        }
    }
}
