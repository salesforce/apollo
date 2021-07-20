
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
 * This holds the log of spurious wakeups and early timeouts.
 * <H2>Description</H2> The <tt>java.lang.Object.wait</tt> method sometimes
 * returns <i>spuriously</i> - i.e. without being <tt>notify</tt>'d by another
 * thread or <tt>interrupt</tt>ed or timed-out! This class is an optional
 * (static) repository holding and reporting counts of any such spurious
 * wakeups. JCSP handles all spurious wakeups cleanly.
 * <p>
 * Some JVMs also timeout on calls of <tt>wait(timeout)</tt> early. This class
 * enables the specification of <i>how early</i> will be acceptable to JCSP.
 * <i>``Timeouts''</i> returned earlier than the set threshold are treated as
 * <i>spurious wakeups</i> (i.e. the process is put back to sleep). Provision is
 * also made for counting and reporting the accepted early timeouts.
 * <p>
 * To operate, this logging must first be switched on ({@link #start}).
 *
 * @author P.H. Welch
 */
//}}}

public class SpuriousLog {

    /**
     * This indexes the counts of spurious wakeups
     * ({@link #getSpuriousWakeUpCounts()}), indicating the class and operation that
     * suffered.
     */
    static public final int One2OneChannelIntRead = 0;

    /**
     * This indexes the counts of spurious wakeups ({@link #report()}), indicating
     * the class and operation that suffered.
     */
    static public final int One2OneChannelIntWrite = 1;

    /**
     * This indexes the counts of spurious wakeups
     * ({@link #getSpuriousWakeUpCounts()}), indicating the class and operation that
     * suffered.
     */
    static public final int One2OneChannelIntXRead = 2;

    /**
     * This indexes the counts of spurious wakeups ({@link #report()}), indicating
     * the class and operation that suffered.
     */
    static public final int One2OneChannelIntXWrite = 3;

    /**
     * This indexes the counts of spurious wakeups
     * ({@link #getSpuriousWakeUpCounts()}), indicating the class and operation that
     * suffered.
     */
    static public final int One2OneChannelRead = 4;

    /**
     * This indexes the counts of spurious wakeups ({@link #report()}), indicating
     * the class and operation that suffered.
     */
    static public final int One2OneChannelWrite = 5;

    /**
     * This indexes the counts of spurious wakeups
     * ({@link #getSpuriousWakeUpCounts()}), indicating the class and operation that
     * suffered.
     */
    static public final int One2OneChannelXRead = 6;

    /**
     * This indexes the counts of spurious wakeups ({@link #report()}), indicating
     * the class and operation that suffered.
     */
    static public final int One2OneChannelXWrite = 7;

    /**
     * This indexes the counts of spurious wakeups
     * ({@link #getSpuriousWakeUpCounts()}), indicating the class and operation that
     * suffered.
     */
    static public final int Any2OneChannelIntRead = 8;

    /**
     * This indexes the counts of spurious wakeups ({@link #report()}), indicating
     * the class and operation that suffered.
     */
    static public final int Any2OneChannelIntWrite = 9;

    /**
     * This indexes the counts of spurious wakeups
     * ({@link #getSpuriousWakeUpCounts()}), indicating the class and operation that
     * suffered.
     */
    static public final int Any2OneChannelIntXRead = 10;

    /**
     * This indexes the counts of spurious wakeups ({@link #report()}), indicating
     * the class and operation that suffered.
     */
    static public final int Any2OneChannelIntXWrite = 11;

    /**
     * This indexes the counts of spurious wakeups
     * ({@link #getSpuriousWakeUpCounts()}), indicating the class and operation that
     * suffered.
     */
    static public final int Any2OneChannelRead = 12;

    /**
     * This indexes the counts of spurious wakeups ({@link #report()}), indicating
     * the class and operation that suffered.
     */
    static public final int Any2OneChannelWrite = 13;

    /**
     * This indexes the counts of spurious wakeups
     * ({@link #getSpuriousWakeUpCounts()}), indicating the class and operation that
     * suffered.
     */
    static public final int Any2OneChannelXRead = 14;

    /**
     * This indexes the counts of spurious wakeups ({@link #report()}), indicating
     * the class and operation that suffered.
     */
    static public final int Any2OneChannelXWrite = 15;

    /**
     * This indexes the counts of spurious wakeups
     * ({@link #getSpuriousWakeUpCounts()}), indicating the class and operation that
     * suffered.
     */
    static public final int One2AnyChannelIntRead = 16;

    /**
     * This indexes the counts of spurious wakeups ({@link #report()}), indicating
     * the class and operation that suffered.
     */
    static public final int One2AnyChannelIntWrite = 17;

    /**
     * This indexes the counts of spurious wakeups
     * ({@link #getSpuriousWakeUpCounts()}), indicating the class and operation that
     * suffered.
     */
    static public final int One2AnyChannelIntXRead = 18;

    /**
     * This indexes the counts of spurious wakeups ({@link #report()}), indicating
     * the class and operation that suffered.
     */
    static public final int One2AnyChannelIntXWrite = 19;

    /**
     * This indexes the counts of spurious wakeups
     * ({@link #getSpuriousWakeUpCounts()}), indicating the class and operation that
     * suffered.
     */
    static public final int One2AnyChannelRead = 20;

    /**
     * This indexes the counts of spurious wakeups ({@link #report()}), indicating
     * the class and operation that suffered.
     */
    static public final int One2AnyChannelWrite = 21;

    /**
     * This indexes the counts of spurious wakeups
     * ({@link #getSpuriousWakeUpCounts()}), indicating the class and operation that
     * suffered.
     */
    static public final int One2AnyChannelXRead = 22;

    /**
     * This indexes the counts of spurious wakeups ({@link #report()}), indicating
     * the class and operation that suffered.
     */
    static public final int One2AnyChannelXWrite = 23;

    /**
     * This indexes the counts of spurious wakeups
     * ({@link #getSpuriousWakeUpCounts()}), indicating the class and operation that
     * suffered.
     */
    static public final int Any2AnyChannelIntRead = 24;

    /**
     * This indexes the counts of spurious wakeups ({@link #report()}), indicating
     * the class and operation that suffered.
     */
    static public final int Any2AnyChannelIntWrite = 25;

    /**
     * This indexes the counts of spurious wakeups
     * ({@link #getSpuriousWakeUpCounts()}), indicating the class and operation that
     * suffered.
     */
    static public final int Any2AnyChannelIntXRead = 26;

    /**
     * This indexes the counts of spurious wakeups ({@link #report()}), indicating
     * the class and operation that suffered.
     */
    static public final int Any2AnyChannelIntXWrite = 27;

    /**
     * This indexes the counts of spurious wakeups
     * ({@link #getSpuriousWakeUpCounts()}), indicating the class and operation that
     * suffered.
     */
    static public final int Any2AnyChannelRead = 28;

    /**
     * This indexes the counts of spurious wakeups ({@link #report()}), indicating
     * the class and operation that suffered.
     */
    static public final int Any2AnyChannelWrite = 29;

    /**
     * This indexes the counts of spurious wakeups
     * ({@link #getSpuriousWakeUpCounts()}), indicating the class and operation that
     * suffered.
     */
    static public final int Any2AnyChannelXRead = 30;

    /**
     * This indexes the counts of spurious wakeups ({@link #report()}), indicating
     * the class and operation that suffered.
     */
    static public final int Any2AnyChannelXWrite = 31;

    /**
     * This indexes the counts of spurious wakeups
     * ({@link #getSpuriousWakeUpCounts()}), indicating the class and operation that
     * suffered.
     */
    static public final int AlternativeSelect = 32;

    /**
     * This indexes the counts of spurious wakeups ({@link #report()}), indicating
     * the class and operation that suffered.
     */
    static public final int AlternativeSelectWithTimeout = 33;

    /**
     * This indexes the counts of spurious wakeups
     * ({@link #getSpuriousWakeUpCounts()}), indicating the class and operation that
     * suffered.
     */
    static public final int BarrierSync = 34;

    /**
     * This indexes the counts of spurious wakeups ({@link #report()}), indicating
     * the class and operation that suffered.
     */
    static public final int BucketFallInto = 35;

    /**
     * This indexes the counts of spurious wakeups
     * ({@link #getSpuriousWakeUpCounts()}), indicating the class and operation that
     * suffered.
     * <p>
     * <i>Note:</i> this field is not operative in the current JCSP release.
     * Spurious wakeups on <tt>AltingBarrier</tt>s are handled correctly -- just not
     * recorded.
     * </p>
     */
    static public final int AltingBarrierCoordinateStartEnable = 36;

    /**
     * This indexes the counts of spurious wakeups ({@link #report()}), indicating
     * the class and operation that suffered.
     */
    static public final int StopRun = 37;

    static private final int nSpuriousWakeUpPlaces = 38;

    static private int[] count = new int[nSpuriousWakeUpPlaces];

    static private int nSpuriousWakeUps = 0;

    static private int nEarlyTimeouts = 0;

    /**
     * Start logging of spurious wakeups. This should be set <i>before</i> any
     * concurrency is started. It should only be set <i>once</i>. There is no
     * concurrency protection!
     */
    static synchronized public void start() {
        Spurious.logging = true;
    }

    /**
     * Finish logging of spurious wakeups. This should be set <i>after</i> any
     * concurrency has finished. There is no concurrency protection!
     */
    static synchronized public void finish() {
        Spurious.logging = false;
    }

    /**
     * Returns the number of spurious wakeups so far.
     *
     * @return the number of spurious wakeups so far.
     */
    static synchronized public int numberSpuriousWakeUps() {
        return nSpuriousWakeUps;
    }

    /**
     * Returns the counts of spurious wakeups so far. This array is indexed by the
     * public constants in this class. Only a clone is returned.
     *
     * @return the counts of spurious wakeups so far.
     */
    static synchronized public int[] getSpuriousWakeUpCounts() {
        return count.clone();
    }

    /**
     * Increment spurious wakeup counts.
     *
     * @param x the operation that suffered the spurious wakeup.
     */
    static synchronized void record(int x) {
        nSpuriousWakeUps++;
        count[x]++;
    }

    /**
     * This sets the allowed early timeout (in msecs). Some JVMs timeout on calls of
     * <tt>wait(timeout)</tt> early - this specifies how early JCSP will tolerate.
     * <i>``Timeouts''</i> returned earlier than the set threshold are treated as
     * <i>spurious wakeups</i> (i.e. the process is put back to sleep).
     * <p>
     * This should be set <i>before</i> any concurrency is started. It should only
     * be set <i>once</i>. There is no concurrency protection!
     *
     * @param earlyTimeout the allowed early timeout (in msecs).
     */
    static synchronized public void setEarlyTimeout(long earlyTimeout) {
        if (earlyTimeout >= 0) {
            Spurious.earlyTimeout = earlyTimeout;
        } else {
            throw new IllegalArgumentException("Attempt to set a negative early timeout value\n");
        }
    }

    /**
     * This returns the allowed early timeout (in msecs).
     *
     * @return the allowed early timeout (in msecs).
     */
    static synchronized public long getEarlyTimeout() {
        return Spurious.earlyTimeout;
    }

    /**
     * Returns the number of early timeouts accepted so far.
     *
     * @return the number of early timeouts accepted so far.
     */
    static synchronized public int numberEarlyTimeouts() {
        return nEarlyTimeouts;
    }

    /**
     * Increment the count of early timeouts.
     */
    static synchronized void incEarlyTimeouts() {
        nEarlyTimeouts++;
    }

    /**
     * This returns a report on the counts of spurious wakeups and early timeouts so
     * far. A breakdown of spurious wakeup counts is given only if there are some.
     *
     * @return the report.
     */
    static synchronized public String report() {
        String result = "\n>>>>>> Spurious WakeUps: " + nSpuriousWakeUps;
        if (nSpuriousWakeUps > 0) {
            for (int i = 0; i < nSpuriousWakeUpPlaces; i++) {
                if ((i % 10) == 0) {
                    result = result + "\n";
                }
                result = result + " " + i + ":" + count[i];
            }
        }
        result = result + "\n>>>>>> Early Timeouts: " + nEarlyTimeouts;
        return result;
    }

    private SpuriousLog() {
    }

}
