
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
 * This is thrown for an illegal operation on an Alternative.
 *
 * <H2>Description</H2> Currently, there is only one cause: we cannot invoke
 * 'priSelect' when there is an AltingBarrier in the Guard array.
 *
 * @author P.H. Welch
 */
//}}}

public class AlternativeError extends Error {

    private static final long serialVersionUID = 1L;

    public AlternativeError(String s) {
        super(s);
    }

}
