
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
 * Defines an interface for a channel output end which may reject data if the
 * reader is not prepared to receive it and calls <code>reject</code> instead of
 * <code>read</code> on the input channel end.
 *
 * @author Quickstone Technologies Limited
 * 
 * @deprecated This channel is superceded by the poison mechanisms, please see
 *             {@link PoisonException}. It remains only because it is used by
 *             some of the networking features.
 */
@Deprecated
public interface RejectableChannelOutput<T> extends ChannelOutput<T> {
    /**
     * Writes data over the channel.
     *
     * @param o an object to write over the channel.
     * @throws ChannelDataRejectedException if the reader rejects the data instead
     *                                      of reading it from the channel.
     */
    @Override
    public void write(T o) throws ChannelDataRejectedException;
}
