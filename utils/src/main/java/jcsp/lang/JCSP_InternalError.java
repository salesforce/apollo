
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
 * This is thrown by an inconsistency detected in the internal structures of
 * JCSP.
 *
 * <H2>Description</H2> Please report the circumstances to jcsp-team@kent.ac.uk
 * - thanks!
 *
 * @author P.H. Welch
 */
//}}}

public class JCSP_InternalError extends Error {
    private static final long serialVersionUID = 1L;

    public JCSP_InternalError(String s) {
        super(s + "\n*** Please report the circumstances to jcsp-team@kent.ac.uk - thanks!");
    }

}
