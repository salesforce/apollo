
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
 * This is a process that starts, engages in no events, performs no computation
 * but refuses to terminate.
 * <H2>Description</H2> <TT>Stop</TT> is a process that starts, engages in no
 * events, performs no computation but refuses to terminate.
 * <p>
 * It can also be used as a {@link Guard} in an {@link Alternative} that is
 * never ready. Of course, this is equivalent to it (and its defended process)
 * not being there at all!
 * <P>
 * <I>Note: this process is included for completeness &ndash; it is one of the
 * fundamental primitives of <B>CSP</B>, where it represents a broken process
 * and is a unit of external choice. In JCSP, it is a unit of
 * {@link Alternative}.</I>
 *
 * @see Skip
 *
 * @author P.D. Austin and P.H. Welch
 *
 */
//}}}

public class Stop extends Guard implements CSProcess {

    /**
     * Enables this guard.
     *
     * @param alt the Alternative doing the enabling.
     */
    @Override
    boolean enable(Alternative alt) {
        Thread.yield();
        return false;
    }

    /**
     * Disables this guard.
     */
    @Override
    boolean disable() {
        return false;
    }

    /**
     * This process starts, engages in no events, performs no computation and
     * refuses to terminate.
     * <p>
     */
    @Override
    public void run() {
        Object lock = new Object();
        synchronized (lock) {
            try {
                lock.wait();
                while (true) {
                    if (Spurious.logging) {
                        SpuriousLog.record(SpuriousLog.StopRun);
                    }
                    lock.wait();
                }

            } catch (InterruptedException e) {
                throw new ProcessInterruptedException("*** Thrown from Stop.run ()\n" + e.toString());
            }
        }
    }

}
