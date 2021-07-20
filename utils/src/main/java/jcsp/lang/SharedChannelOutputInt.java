
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
 * This is the same as {@link ChannelOutputInt} except that it is guaranteed
 * safe to pass on to more than one internal process for parallel writing.
 * <p>
 * A <i>writing-end</i>, obtained from an <i>any-one</i> or <i>any-any</i>
 * channel by invoking its <tt>in()</tt> method, will implement this interface.
 *
 * @author Quickstone Technologies Limited
 */
public interface SharedChannelOutputInt extends ChannelOutputInt {
}
