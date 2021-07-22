
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

import jcsp.lang.SharedChannelInput;

/**
 * This is wrapper for a <code>SharedChannelInput</code> that adds read
 * filtering. Instances of this class can be safely used by multiple concurrent
 * processes.
 *
 * @author Quickstone Technologies Limited
 */
public class FilteredSharedChannelInputWrapper<T> extends FilteredChannelInputWrapper<T>
                                              implements FilteredSharedChannelInput<T> {
    /**
     * The object used for synchronization by the methods here to protect the
     * readers from each other when manipulating the filters and reading data.
     */
    private Object synchObject;

    /**
     * Constructs a new wrapper for the given channel input end.
     *
     * @param in the existing channel end.
     */
    public FilteredSharedChannelInputWrapper(SharedChannelInput<T> in) {
        super(in);
        synchObject = new Object();
    }

    @Override
    public T read() {
        synchronized (synchObject) {
            return super.read();
        }
    }

    @Override
    public void addReadFilter(Filter filter) {
        synchronized (synchObject) {
            super.addReadFilter(filter);
        }
    }

    @Override
    public void addReadFilter(Filter filter, int index) {
        synchronized (synchObject) {
            super.addReadFilter(filter, index);
        }
    }

    @Override
    public void removeReadFilter(Filter filter) {
        synchronized (synchObject) {
            super.removeReadFilter(filter);
        }
    }

    @Override
    public void removeReadFilter(int index) {
        synchronized (synchObject) {
            super.removeReadFilter(index);
        }
    }

    @Override
    public Filter getReadFilter(int index) {
        synchronized (synchObject) {
            return super.getReadFilter(index);
        }
    }

    @Override
    public int getReadFilterCount() {
        synchronized (synchObject) {
            return super.getReadFilterCount();
        }
    }
}
