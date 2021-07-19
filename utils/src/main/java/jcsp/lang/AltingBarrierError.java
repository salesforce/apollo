
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

//{{{  javadoc

/**
 * This is thrown for an illegal operation on an {@link AltingBarrier}.
 *
 * <H2>Description</H2> Currently, there are the following causes:
 * <UL>
 * <LI>different threads trying to operate on the same front-end;
 * <LI>attempt to use as a {@link Guard} whilst resigned;
 * <LI>attempt to {@link AltingBarrier#sync sync} whilst resigned;
 * <LI>attempt to {@link AltingBarrier#resign resign} whilst resigned;
 * <LI>attempt to {@link AltingBarrier#enroll enroll} whilst enrolled;
 * <LI>attempt to {@link AltingBarrier#expand expand} whilst resigned;
 * <LI>attempt to {@link AltingBarrier#contract contract} whilst resigned;
 * <LI>attempt to {@link AltingBarrier#contract contract} with an array of
 * front-ends not supplied by {@link AltingBarrier#expand expand};
 * <LI>attempt to {@link AltingBarrier#mark mark} whilst resigned (caused by a
 * process transfering a <i>front-end</i> in that state).
 * </UL>
 *
 * @author P.H. Welch
 */
//}}}

public class AltingBarrierError extends Error {
    private static final long serialVersionUID = 1L;

    public AltingBarrierError(String s) {
        super(s);
    }

}
