
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

class AltingBarrierCoordinate { // package-only visible class

    /*
     * This records number of processes active in ALT enable/disable sequences
     * involving a barrier. <P> Only one process may be engaged in an enable
     * sequence involving a barrier. <P> Disable sequences, triggered by a
     * successful barrier enable, may happen in parallel. Disable sequences,
     * triggered by a successful barrier enable, may not happen in parallel with an
     * enable sequence involving a barrier. <P> Disable sequences involving a
     * barrier, triggered by a successful non-barrier enable, may happen in parallel
     * with an enable sequence involving a barrier. Should the enable sequence
     * complete a barrier that is in a disable sequence (which can't yet have been
     * disabled, else it could not have been completed), the completed barrier will
     * be found (when it is disabled) and that disable sequence becomes as though it
     * had been triggered by that successful barrier enable (rather than the
     * non-barrier event).
     */
    private static int active = 0;

    /** Lock object for coordinating enable/disable sequences. */
    private static Object activeLock = new Object();

    /* Invoked at start of an enable sequence involving a barrier. */
    static void startEnable() {
        synchronized (activeLock) {
            if (active > 0) {
                try {
                    activeLock.wait();
                    while (active > 0) {
                        // This may be a spurious wakeup. More likely, this is a properly
                        // notified wakeup that has been raced to the 'activelock' monitor
                        // by another thread (quite possibly the notifying one) that has
                        // (re-)acquired it and set 'active' greater than zero. We have
                        // not instrumented the code to tell the difference. Either way:
                        activeLock.wait();
                    }
                } catch (InterruptedException e) {
                    throw new ProcessInterruptedException(e.toString());
                }
            }
            if (active != 0) {
                throw new JCSP_InternalError("\n*** AltingBarrier enable sequence starting "
                + "with 'active' count not equal to zero: " + active);
            }
            active = 1;
        }
    }

    /* Invoked at finish of an unsuccessful enable sequence involving a barrier. */
    static void finishEnable() {
        synchronized (activeLock) {
            if (active != 1) {
                throw new JCSP_InternalError("\n*** AltingBarrier enable sequence finished "
                + "with 'active' count not equal to one: " + active);
            }
            active = 0;
            activeLock.notify();
        }
    }

    /*
     * Invoked by a successful barrier enable.
     *
     * @param n The number of processes being released to start their disable
     * sequences.
     */
    static void startDisable(int n) {
        if (n <= 0) {
            throw new JCSP_InternalError("\n*** attempt to start " + n + " disable sequences!");
        }
        synchronized (activeLock) { // not necessary ... ?
            if (active != 1) {
                throw new JCSP_InternalError("\n*** completed AltingBarrier found in ALT sequence "
                + "with 'active' count not equal to one: " + active);
            }
            active = n;
        }
    }

    /* Invoked at finish of a disable sequence selecting a barrier. */
    static void finishDisable() {
        synchronized (activeLock) {
            if (active < 1) {
                throw new JCSP_InternalError("\n*** AltingBarrier disable sequence finished "
                + "with 'active' count less than one: " + active);
            }
            active--;
            if (active == 0) {
                activeLock.notify();
            }
        }
    }

}
