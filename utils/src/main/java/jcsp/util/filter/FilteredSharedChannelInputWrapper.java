
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
 * This is wrapper for a <code>SharedChannelInput</code> that adds
 * read filtering. Instances of this class can be safely used by
 * multiple concurrent processes.
 *
 * @author Quickstone Technologies Limited
 */
public class FilteredSharedChannelInputWrapper
        extends FilteredChannelInputWrapper
        implements FilteredSharedChannelInput
{    
    /**
     * The object used for synchronization by the methods here to protect the readers from each other
     * when manipulating the filters and reading data.
     */
    private Object synchObject;

    /**
     * Constructs a new wrapper for the given channel input end.
     *
     * @param in the existing channel end.
     */
    public FilteredSharedChannelInputWrapper(SharedChannelInput in)
    {
        super(in);        
        synchObject = new Object();
    }

    public Object read()
    {
        synchronized (synchObject)
        {
            return super.read();
        }
    }

    public void addReadFilter(Filter filter)
    {
        synchronized (synchObject)
        {
            super.addReadFilter(filter);
        }
    }

    public void addReadFilter(Filter filter, int index)
    {
        synchronized (synchObject)
        {
            super.addReadFilter(filter, index);
        }
    }

    public void removeReadFilter(Filter filter)
    {
        synchronized (synchObject)
        {
            super.removeReadFilter(filter);
        }
    }

    public void removeReadFilter(int index)
    {
        synchronized (synchObject)
        {
            super.removeReadFilter(index);
        }
    }

    public Filter getReadFilter(int index)
    {
        synchronized (synchObject)
        {
            return super.getReadFilter(index);
        }
    }

    public int getReadFilterCount()
    {
        synchronized (synchObject)
        {
            return super.getReadFilterCount();
        }
    }
}
