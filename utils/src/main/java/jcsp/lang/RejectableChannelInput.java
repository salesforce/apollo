
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
 * Defines an interface for an input channel end that gives the reader the
 * ability to reject instead of accepting pending data.
 *
 * @author Quickstone Technologies Limited
 * 
 * @deprecated This channel is superceded by the poison mechanisms, please see
 *             {@link PoisonException}. It remains only because it is used by
 *             some of the networking features.
 */
@Deprecated
public interface RejectableChannelInput<T> extends ChannelInput<T> {
    /**
     * Reject any data pending instead of reading it. The currently blocked writer
     * will receive a <Code>ChannelDataRejectedException</code>.
     */
    public void reject();
}
