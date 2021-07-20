
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
 * This is a process that immediately terminates <I>and</I> a {@link Guard} that
 * is always ready.
 * <H2>Description</H2> <TT>Skip</TT> is a process that starts, engages in no
 * events, performs no computation and terminates.
 * <p>
 * It can also be used as a {@link Guard} in an {@link Alternative} that is
 * always ready. This makes it useful for
 * <a href="Alternative.html#Polling"><i>polling</i></a> a set of guards to test
 * if any are ready: include it as the last element of the guard array and
 * {@link Alternative#priSelect() priSelect}.
 * <P>
 * <I>Note: the process is also included for completeness &ndash; it is one of
 * the fundamental primitives of <B>CSP</B>, where it is a unit of sequential
 * composition and parallel interleaving. In JCSP, it is a unit of
 * {@link Sequence}, {@link Parallel} and {@link PriParallel} .</I>
 *
 * @see Stop
 *
 * @author P.D. Austin
 * @author P.H. Welch
 *
 */

public class Skip extends Guard implements CSProcess {
    /**
     * Enables this guard.
     *
     * @param alt the Alternative doing the enabling.
     */
    @Override
    boolean enable(Alternative alt) {
        Thread.yield();
        return true;
    }

    /**
     * Disables this guard.
     */
    @Override
    boolean disable() {
        return true;
    }

    /**
     * The main body of this process.
     */
    @Override
    public void run() {
    }
}
