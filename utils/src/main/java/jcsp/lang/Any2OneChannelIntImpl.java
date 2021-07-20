
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
 * This implements an any-to-one integer channel, safe for use by many writers
 * and one reader.Refer to the {@link Any2OneChannelInt} interface for a fuller
 * description.
 *
 * @see jcsp.lang.One2OneChannelIntImpl
 * @see jcsp.lang.One2AnyChannelIntImpl
 * @see Any2AnyChannelIntImpl
 * @see jcsp.util.ints.ChannelDataStoreInt
 *
 * @author P.D. Austin and P.H. Welch
 */

class Any2OneChannelIntImpl extends Any2OneIntImpl {
    Any2OneChannelIntImpl() {
        super(new One2OneChannelIntImpl());
    }
}
