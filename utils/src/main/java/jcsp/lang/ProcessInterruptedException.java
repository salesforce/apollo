
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
 * This is thrown if a process is interrupted whilst blocked during
 * synchronisation - processes should never be interrupted.
 *
 * <H2>Description</H2> This is caused by accessing the Java thread executing a
 * JCSP process and invoking its <TT>java.lang.Thread.interrupt</TT>() method.
 * If this is done to a process blocked on a JCSP synchronisation primitive
 * (such as a channel communication or timeout), the process will wake up
 * prematurely -- invalidating the semantics of that primitive. The wake up is
 * intercepted and this {@link Error} is thrown.
 * <P>
 * Some browsers, when shutting down an <I>applet</I>, may do this to processes
 * spawned by an {@link jcsp.awt.ActiveApplet} that have not died naturally.
 *
 * Alternatively, this may be raised by processes stopped prematurely as a
 * result of a call to <TT>Parallel.destroy</TT>, or by calling <TT>stop</TT> on
 * the <TT>ProcessManager</TT> responsible for the process (or network).
 *
 * @author P.H. Welch
 */
//}}}

public class ProcessInterruptedException extends Error {
    private static final long serialVersionUID = 1L;

    public ProcessInterruptedException(String s) {
        super("\n*** Interrupting a running process is not compatible with JCSP\n" + "*** Please don't do this!\n" + s);
// System.out.println ("Someone is creating a ProcessInterruptedException!\n" + s);
    }

}
