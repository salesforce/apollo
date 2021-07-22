
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

package jcsp.lang;

/**
 * Defines a wrapper to go around a channel output end. This wrapper allows a
 * channel end to be given away without any risk of the user of that end casting
 * it to a channel input because they cannot gain access to the actual channel
 * end.
 *
 * @deprecated There is no longer any need to use this class, after the 1.1
 *             class reorganisation.
 *
 * @author Quickstone Technologies Limited
 */
@Deprecated
public class ChannelOutputWrapper<T> implements ChannelOutput<T> {
    /**
     * The actual channel end.
     */
    private ChannelOutput<T> out;

    /**
     * Creates a new wrapper for the given channel end.
     *
     * @param out the existing channel end.
     */
    public ChannelOutputWrapper(ChannelOutput<T> out) {
        this.out = out;
    }

    /**
     * Writes a value to the channel.
     *
     * @param o the value to write.
     * @see ChannelOutput
     */
    @Override
    public void write(T o) {
        out.write(o);
    }

    @Override
    public void poison(int strength) {
        out.poison(strength);
    }

}
