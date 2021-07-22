
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
import jcsp.lang.Any2OneChannel;
import jcsp.lang.BufferedChannelArrayFactory;
import jcsp.lang.BufferedChannelFactory;
import jcsp.lang.ChannelArrayFactory;
import jcsp.lang.ChannelFactory;
import jcsp.lang.One2AnyChannel;
import jcsp.lang.One2OneChannel;
import jcsp.lang.StandardChannelFactory;
import jcsp.util.ChannelDataStore;

/**
 * <p>
 * This class is used for constructing Filtered Channels. The objects returned
 * by instances of this class will implement the appropriate Filtered Channel
 * interfaces even though the return types are not declared as being Filtered
 * Channels. This is so that this class can implement the
 * <code>ChannelFactory</code> and <code>ChannelArrayFactory</code> interfaces.
 * Instances of this class can therefore be used in place of the standard
 * channel factory classes.
 * </p>
 *
 * <p>
 * A set of read and/or write filters can be specified so that all of the
 * channels created by this factory will have the same buffering properties.
 * </p>
 *
 * @author Quickstone Technologies Limited
 */
public class FilteredChannelFactory implements ChannelFactory, ChannelArrayFactory, BufferedChannelFactory,
                                    BufferedChannelArrayFactory {
    /**
     * Underlying factory for creating the base channels.
     */
    private StandardChannelFactory factory;

    /**
     * Read filters to install in channels created by this factory.
     */
    private Filter[] readFilters;

    /**
     * Write filters to install in channels created by this factory.
     */
    private Filter[] writeFilters;

    /**
     * All channels constructed with a Factory constructed with this constructor
     * will default to having no pre-installed filters.
     *
     */
    public FilteredChannelFactory() {
        factory = new StandardChannelFactory();
    }

    /**
     * <p>
     * All channels constructed with this Factory instance will have the specified
     * <code>Filter</code> objects inserted into them. The same instances of the
     * filters will be inserted into each channel.
     * </p>
     *
     * <p>
     * Either of the parameters may be <code>null</code> if read/write filters are
     * not required.
     * </p>
     *
     * @param readFilters  optional read filters to install in new channels.
     * @param writeFilters optional write filters to install in new channels.
     */
    public FilteredChannelFactory(Filter[] readFilters, Filter[] writeFilters) {
        this();
        this.readFilters = readFilters;
        this.writeFilters = writeFilters;
    }

    /**
     * Installs the filters currently set for this factory into the read/write
     * channel ends supplied.
     *
     * @param readFiltered  optional control interface for the read end of a
     *                      filtered channel.
     * @param writeFiltered optional control interface for the write end of a
     *                      filtered channel.
     */
    private void installFilters(ReadFiltered readFiltered, WriteFiltered writeFiltered) {
        if (readFilters != null)
            for (int i = 0; i < readFilters.length; i++)
                readFiltered.addReadFilter(readFilters[i]);
        if (writeFilters != null)
            for (int i = 0; i < writeFilters.length; i++)
                writeFiltered.addWriteFilter(writeFilters[i]);
    }

    /**
     * Creates a new One2One channel with the filtering options set for this
     * factory.
     *
     * @return the created channel with the filters installed.
     */
    @Override
    public One2OneChannel createOne2One() {
        FilteredOne2OneChannelImpl toReturn = new FilteredOne2OneChannelImpl(factory.createOne2One());
        installFilters(toReturn.inFilter(), toReturn.outFilter());
        return toReturn;
    }

    /**
     * Creates a new Any2One channel with the filtering options set for this
     * factory.
     *
     * @return the created channel with the filters installed.
     */
    @Override
    public Any2OneChannel createAny2One() {
        FilteredAny2OneChannelImpl toReturn = new FilteredAny2OneChannelImpl(factory.createAny2One());
        installFilters(toReturn.inFilter(), toReturn.outFilter());
        return toReturn;
    }

    /**
     * Creates a new One2Any channel with the filtering options set for this
     * factory.
     *
     * @return the created channel with the filters installed.
     */
    @Override
    public One2AnyChannel createOne2Any() {
        FilteredOne2AnyChannelImpl toReturn = new FilteredOne2AnyChannelImpl(factory.createOne2Any());
        installFilters(toReturn.inFilter(), toReturn.outFilter());
        return toReturn;
    }

    /**
     * Creates a new Any2Any channel with the filtering options set for this
     * factory.
     *
     * @return the created channel with the filters installed.
     */
    @Override
    public Any2AnyChannel createAny2Any() {
        FilteredAny2AnyChannelImpl toReturn = new FilteredAny2AnyChannelImpl(factory.createAny2Any());
        installFilters(toReturn.inFilter(), toReturn.outFilter());
        return toReturn;
    }

    /**
     * Constructs and returns an array of <code>One2OneChannel</code> objects.
     *
     * @param n the size of the array of channels.
     * @return the array of channels.
     *
     * @see ChannelArrayFactory#createOne2One(int)
     */
    @Override
    public One2OneChannel[] createOne2One(int n) {
        One2OneChannel[] toReturn = new One2OneChannel[n];
        for (int i = 0; i < n; i++) {
            toReturn[i] = createOne2One();
        }
        return toReturn;
    }

    /**
     * Constructs and returns an array of <code>Any2OneChannel</code> objects.
     *
     * @param n the size of the array of channels.
     * @return the array of channels.
     *
     * @see ChannelArrayFactory#createAny2One(int)
     */
    @Override
    public Any2OneChannel[] createAny2One(int n) {
        Any2OneChannel[] toReturn = new Any2OneChannel[n];
        for (int i = 0; i < n; i++)
            toReturn[i] = createAny2One();
        return toReturn;
    }

    /**
     * Constructs and returns an array of <code>One2AnyChannel</code> objects.
     *
     * @param n the size of the array of channels.
     * @return the array of channels.
     *
     * @see ChannelArrayFactory#createOne2Any(int)
     */
    @Override
    public One2AnyChannel[] createOne2Any(int n) {
        One2AnyChannel[] toReturn = new One2AnyChannel[n];
        for (int i = 0; i < n; i++)
            toReturn[i] = createOne2Any();
        return toReturn;
    }

    /**
     * Constructs and returns an array of <code>Any2AnyChannel</code> objects.
     *
     * @param n the size of the array of channels.
     * @return the array of channels.
     *
     * @see ChannelArrayFactory#createAny2Any(int)
     */
    @Override
    public Any2AnyChannel[] createAny2Any(int n) {
        Any2AnyChannel[] toReturn = new Any2AnyChannel[n];
        for (int i = 0; i < n; i++)
            toReturn[i] = createAny2Any();
        return toReturn;
    }

    /**
     * Creates a new One2One channel with the filtering options set for this factory
     * and the specified data buffer.
     *
     * @param buffer the buffer implementation to use.
     * @return the created filtered channel.
     */
    @Override
    public One2OneChannel createOne2One(ChannelDataStore buffer) {
        FilteredOne2OneChannelImpl toReturn = new FilteredOne2OneChannelImpl(factory.createOne2One(buffer));
        installFilters(toReturn.inFilter(), toReturn.outFilter());
        return toReturn;
    }

    /**
     * Creates a new Any2One channel with the filtering options set for this factory
     * and the specified data buffer.
     *
     * @param buffer the buffer implementation to use.
     * @return the created filtered channel.
     */
    @Override
    public Any2OneChannel createAny2One(ChannelDataStore buffer) {
        FilteredAny2OneChannelImpl toReturn = new FilteredAny2OneChannelImpl(factory.createAny2One(buffer));
        installFilters(toReturn.inFilter(), toReturn.outFilter());
        return toReturn;
    }

    /**
     * Creates a new One2Any channel with the filtering options set for this factory
     * and the specified data buffer.
     *
     * @param buffer the buffer implementation to use.
     * @return the created filtered channel.
     */
    @Override
    public One2AnyChannel createOne2Any(ChannelDataStore buffer) {
        FilteredOne2AnyChannelImpl toReturn = new FilteredOne2AnyChannelImpl(factory.createOne2Any(buffer));
        installFilters(toReturn.inFilter(), toReturn.outFilter());
        return toReturn;
    }

    /**
     * Creates a new Any2Any channel with the filtering options set for this factory
     * and the specified data buffer.
     *
     * @param buffer the buffer implementation to use.
     * @return the created filtered channel.
     */
    @Override
    public Any2AnyChannel createAny2Any(ChannelDataStore buffer) {
        FilteredAny2AnyChannelImpl toReturn = new FilteredAny2AnyChannelImpl(factory.createAny2Any(buffer));
        installFilters(toReturn.inFilter(), toReturn.outFilter());
        return toReturn;
    }

    /**
     * Constructs and returns an array of <code>One2OneChannel</code> objects with a
     * given buffering behaviour.
     *
     * @param n      the size of the array of channels.
     * @param buffer the buffer implementation to use.
     * @return the array of channels.
     *
     * @see ChannelArrayFactory#createOne2One(int)
     */
    @Override
    public One2OneChannel[] createOne2One(ChannelDataStore buffer, int n) {
        One2OneChannel[] toReturn = new One2OneChannel[n];
        for (int i = 0; i < n; i++)
            toReturn[i] = createOne2One(buffer);
        return toReturn;
    }

    /**
     * Constructs and returns an array of <code>Any2OneChannel</code> objects with a
     * given buffering behaviour.
     *
     * @param n      the size of the array of channels.
     * @param buffer the buffer implementation to use.
     * @return the array of channels.
     *
     * @see ChannelArrayFactory#createAny2One(int)
     */
    @Override
    public Any2OneChannel[] createAny2One(ChannelDataStore buffer, int n) {
        Any2OneChannel[] toReturn = new Any2OneChannel[n];
        for (int i = 0; i < n; i++)
            toReturn[i] = createAny2One(buffer);
        return toReturn;
    }

    /**
     * Constructs and returns an array of <code>One2AnyChannel</code> objects with a
     * given buffering behaviour.
     *
     * @param n      the size of the array of channels.
     * @param buffer the buffer implementation to use.
     * @return the array of channels.
     *
     * @see ChannelArrayFactory#createOne2Any(int)
     */
    @Override
    public One2AnyChannel[] createOne2Any(ChannelDataStore buffer, int n) {
        One2AnyChannel[] toReturn = new One2AnyChannel[n];
        for (int i = 0; i < n; i++)
            toReturn[i] = createOne2Any(buffer);
        return toReturn;
    }

    /**
     * Constructs and returns an array of <code>Any2AnyChannel</code> objects with a
     * given buffering behaviour.
     *
     * @param n      the size of the array of channels.
     * @param buffer the buffer implementation to use.
     * @return the array of channels.
     *
     * @see ChannelArrayFactory#createAny2Any(int)
     */
    @Override
    public Any2AnyChannel[] createAny2Any(ChannelDataStore buffer, int n) {
        Any2AnyChannel[] toReturn = new Any2AnyChannel[n];
        for (int i = 0; i < n; i++)
            toReturn[i] = createAny2Any(buffer);
        return toReturn;
    }
}
