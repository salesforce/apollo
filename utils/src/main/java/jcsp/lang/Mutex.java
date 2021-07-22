
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
 * A package-visible class that implements a straightforward mutex, for use by
 * One2AnyChannel and Any2AnyChannel
 * 
 * @author N.C.C. Brown
 *
 */
class Mutex {

    private boolean claimed = false;

    public void claim() {
        synchronized (this) {
            while (claimed) {
                try {
                    wait();
                } catch (InterruptedException e) {
                    throw new ProcessInterruptedException("*** Thrown from Mutex.claim()\n" + e.toString());
                }
            }
            claimed = true;
        }
    }

    public void release() {
        synchronized (this) {
            claimed = false;
            notify();
        }
    }

}
