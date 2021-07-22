
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
 * Defines a wrapper to go around a channel input end. This wrapper allows a
 * channel end to be given away without any risk of the user of that end casting
 * it to a channel output because they cannot gain access to the actual channel
 * end.
 *
 * @deprecated There is no longer any need to use this class, after the 1.1
 *             class reorganisation.
 *
 * @author Quickstone Technologies Limited
 */
@Deprecated
public class ChannelInputWrapper<T> implements ChannelInput<T> {
    /**
     * The actual channel end.
     */
    private ChannelInput<T> in;

    /**
     * Constructs a new wrapper around the given channel end.
     *
     * @param in the existing channel end.
     */
    public ChannelInputWrapper(ChannelInput<T> in) {
        this.in = in;
    }

    /**
     * Reads a value from the channel.
     *
     * @see ChannelInput
     * @return the value read.
     */
    @Override
    public T read() {
        return in.read();
    }

    /**
     * Begins an extended rendezvous
     * 
     * @see ChannelInput.startRead
     * @return The object read from the channel
     */
    @Override
    public T startRead() {
        return in.startRead();
    }

    /**
     * Ends an extended rendezvous
     * 
     * @see ChannelInput.endRead
     */
    @Override
    public void endRead() {
        in.endRead();
    }

    @Override
    public void poison(int strength) {
        in.poison(strength);
    }

}
