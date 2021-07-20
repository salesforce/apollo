
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

import java.io.Serializable;

/**
 * This enables <I>barrier</I> synchronisation between a set of processes.
 * <P>
 * <A HREF="#constructor_summary">Shortcut to the Constructor and Method
 * Summaries.</A>
 * 
 * <H2>Description</H2> A channel is a CSP <I>event</I> in which only two
 * processes (the reader and the writer) synchronise. A barrier is a CSP
 * <I>event</I> in which any number of processes may synchronise. <I>Any</I>
 * process synchronising on a barrier will be blocked until <I>all</I> processes
 * associated with that barrier have synchronised. A process may not back off an
 * attempted synchronisation - i.e. barriers cannot be used as guards in an
 * {@link Alternative}. Applications needing that capability should use an
 * {@link AltingBarrier}.
 * <P>
 * A process network may contain many barriers - each being associated with a
 * different subset of processes. These subsets may overlap and change at
 * run-time. JCSP does not currently provide a checked way of associating a
 * process with a particular barrier. That could be done, but would carry a
 * syntactic and run-time overhead currently not thought worthwhile. Associating
 * a barrier with its correct set of processes is a design issue and it is left
 * to the designer to ensure correct useage.
 * <P>
 * <I>Note:</I> this notion of barrier corresponds to the <A HREF=
 * "http://frmb.org/occ21-extensions-clean.html#barrier"><tt>BARRIER</tt></A>
 * synchronisation primitive added to the
 * <A HREF="http://www.cs.kent.ac.uk/projects/ofa/kroc/">KRoC</A>
 * <A HREF="http://occam-pi.org/"><B>occam-pi</B></A> language system.
 * 
 * <H3>Deterministic Barriers</H3> If the set of processes associated with a
 * barrier remains fixed, barrier synchronisation introduces no
 * <I>non-determinism</I>. So, a parallel system made up of processes, barriers
 * (with fixed barrier sets) and 1-1 channels (with no ALTing on the channels
 * and an exclusive read/write access discipline for all communicated objects)
 * is deterministic - its semantics are independent of scheduling.
 * <P>
 * For fixed barrier sets, {@link #Barrier(int) <R>construct</R>} each barrier
 * initialised to the number of processes to be associated with it and share it
 * out amongst those processes.
 * <P>
 * For example, here is a fixed set of 10 processes synchronising on a shared
 * barrier:
 * <p>
 * <IMG SRC="doc-files\Barrier1.gif">
 * </p>
 * Here is the JCSP code for this network:
 * 
 * <PRE>
 * import jcsp.lang.*;
 * 
 * public class BarrierExample1 {
 * 
 *     public static void main(String[] args) {
 * 
 *         final int nPlayers = 10;
 * 
 *         final Barrier barrier = new Barrier(nPlayers);
 * 
 *         final Player[] players = new Player[nPlayers];
 *         for (int i = 0; i < players.length; i++) {
 *             players[i] = new Player(i, nPlayers, barrier);
 *         }
 * 
 *         new Parallel(players).run();
 * 
 *     }
 * 
 * }
 * </PRE>
 * 
 * To synchronise on a barrier, a process just needs to invoke its {@link #sync
 * sync} method. For example:
 * 
 * <PRE>
 * import jcsp.lang.*;
 * 
 * public class Player implements CSProcess {
 * 
 *     private final int id, nPlayers;
 *     private final Barrier barrier;
 * 
 *     public Player(int id, int nPlayers, Barrier barrier) {
 *         this.id = id;
 *         this.nPlayers = nPlayers;
 *         this.barrier = barrier;
 *     }
 * 
 *     public void run() {
 *         final CSTimer tim = new CSTimer();
 *         final long second = 1000; // JCSP timer units are milliseconds
 *         int busy = id + 1;
 *         while (true) {
 *             tim.sleep(busy * second); // application specific work
 *             System.out.println("Player " + id + " at the barrier ...");
 *             barrier.sync();
 *             System.out.println("\t\t\t... Player " + id + " over the barrier");
 *             busy = (nPlayers + 1) - busy; // just to make it more interesting
 *         }
 *     }
 * 
 * }
 * </PRE>
 * 
 * The <TT>sleep</TT> period above represents some work carried out by each
 * <TT>Player</TT>. This work takes a different amount of time in each cycle and
 * varies from player to player. At the end of each piece of work, each player
 * waits for all its colleagues before continuing its next cycle.
 * 
 * <H3><A NAME="ND-barrier">Non-Deterministic Barriers</H3> A process may choose
 * at any time to {@link #enroll enroll} or {@link #resign resign} from any
 * barrier it can see. It should not, of course, <TT>enroll</TT> on a barrier
 * with which it is already associated - nor <TT>resign</TT> from a barrier with
 * which it isn't! Because these operations are internal choices of individual
 * processes and because they have an impact on the synchronisation properties
 * of their environment, the resulting system is non-deterministic.
 * <A NAME="Worker-TimeKeeper">
 * <p>
 * <IMG SRC="doc-files\Barrier2.gif">
 * </p>
 * In the above example, <TT>Worker</TT> processes cycle between <I>working</I>
 * and <I>resting</I> states, making their own decisions about when to switch.
 * When <I>working</I>, they enroll in a barrier shared with a
 * <TT>TimeKeeper</TT> process - when <I>resting</I>, they resign from this
 * barrier. Whilst <I>working</I> and after they have enrolled, they execute a
 * sequence of <I>work units</I> triggered by synchronisations on the barrier.
 * <P>
 * The <TT>TimeKeeper</TT> synchronises on the barrier at a regular rate (once
 * per second) and, thus, coordinates the activities of all <I>working</I>
 * <TT>Worker</TT>s. A <I>work unit</I> can only start at the beginning of one
 * of the <TT>TimeKeeper</TT>'s time slots and each <TT>Worker</TT> can only
 * perform one <I>work unit</I> per time slot. Should any <I>work unit</I>
 * overrun a time slot, subsequent units (for all <TT>Worker</TT>s) will have a
 * late start. However, the system is stable - so long as there is some slack in
 * the system (i.e. units do not <I>generally</I> overrun), the original
 * schedule will be recovered.
 * <P>
 * Here is the code for the complete system. The <TT>barrier</TT> is initialised
 * to just <TT>1</TT>, since only the <TT>TimeKeeper</TT> is permanently
 * associated with it. The <TT>barrier</TT> is passed to all <TT>Worker</TT>s as
 * well as to the <TT>TimeKeeper</TT>:
 * 
 * <PRE>
 * import jcsp.lang.*;
 * 
 * public class BarrierExample2 {
 * 
 *     public static void main(String[] args) {
 * 
 *         final int nWorkers = 10;
 *         final int rogue = 5;
 * 
 *         final int second = 1000;
 *         // JCSP timer units are milliseconds
 *         final int tick = 1 * second;
 *         final int maxWork = tick;
 *         // raise this to allow workers to overrun
 * 
 *         final long seed = new CSTimer().read();
 * 
 *         final Barrier barrier = new Barrier(1);
 * 
 *         final TimeKeeper timeKeeper = new TimeKeeper(tick, barrier);
 * 
 *         final Worker[] workers = new Worker[nWorkers];
 *         for (int i = 0; i < workers.length; i++) {
 *             workers[i] = new Worker(i, i + seed, maxWork, i == rogue, barrier);
 *         }
 * 
 *         new Parallel(new CSProcess[] { timeKeeper, new Parallel(workers) }).run();
 *     }
 * }
 * </PRE>
 * 
 * As well as the <TT>barrier</TT>, each <TT>Worker</TT> is given its
 * <TT>id</TT>, a (unique) <TT>seed</TT> for its random number generator, its
 * maximum work unit time and whether it is a <TT>rogue</TT>. A rogue worker
 * deliberately overruns its last unit of work for each working session to test
 * out the stability of the system:
 * 
 * <PRE>
 * import jcsp.lang.*;
 * import java.util.*;
 * 
 * public class Worker implements CSProcess {
 * 
 *     private final int id;
 *     private final long seed;
 *     private final int maxWork;
 *     private final boolean rogue;
 *     private final Barrier barrier;
 * 
 *     public Worker(int id, long seed, int maxWork, boolean rogue, Barrier barrier) {
 *         this.id = id;
 *         this.seed = seed;
 *         this.maxWork = maxWork;
 *         this.rogue = rogue;
 *         this.barrier = barrier;
 *     }
 * 
 *     public void run() {
 * 
 *         final Random random = new Random(seed);
 *         // each process gets a different seed
 * 
 *         final CSTimer tim = new CSTimer();
 *         final int second = 1000;
 *         // JCSP timer units are milliseconds
 * 
 *         final int minRest = 3 * second;
 *         final int maxRest = (id + 10) * second;
 *         final int nWorkUnits = id + 1;
 * 
 *         final String starting = "\tWorker " + id + " starting ...";
 *         final String working = "\t\t\t  ... Worker " + id + " working ...";
 *         final String resting = "\t\t\t\t\t       ... Worker " + id + " resting ...";
 * 
 *         while (true) {
 *             barrier.enroll();
 *             System.out.println(starting);
 *             for (int i = 0; i < nWorkUnits; i++) {
 *                 barrier.sync();
 *                 System.out.println(working);
 *                 tim.sleep(random.nextInt(maxWork));
 *                 // these lines represent one unit of work
 *             }
 *             if (rogue)
 *                 tim.sleep(maxWork);
 *             // try to throw the timekeeper
 *             barrier.resign();
 *             System.out.println(resting);
 *             tim.sleep(minRest + random.nextInt(maxRest));
 *         }
 *     }
 * 
 * }
 * </PRE>
 * 
 * Note that the {@link #resign resign} method also performs a (non-blocking)
 * synchronisation on the barrier as well as the resignation. This is crucial
 * since, if the resigner were the last process associated with a barrier not to
 * have invoked a {@link #sync sync}, its resignation must <I>complete</I> the
 * barrier (as though it had invoked a <TT>sync</TT>) and release all the
 * remaining associated processes.
 * <P>
 * The <TT>TimeKeeper</TT> is passed its <TT>tick</TT> interval and the
 * <TT>Barrier</TT>. It is pre-enrolled with the <TT>Barrier</TT> and remains
 * permanently associated:
 * 
 * <PRE>
 * import jcsp.lang.*;
 * 
 * public class TimeKeeper implements CSProcess {
 * 
 *     private final long interval;
 *     private final Barrier barrier;
 * 
 *     public TimeKeeper(long interval, Barrier barrier) {
 *         this.interval = interval;
 *         this.barrier = barrier;
 *     }
 * 
 *     public void run() {
 * 
 *         final CSTimer tim = new CSTimer();
 *         long timeout = tim.read() + interval;
 * 
 *         while (true) {
 *             tim.after(timeout);
 *             barrier.sync();
 *             System.out.println("[" + (tim.read() - timeout) + "]");
 *             timeout += interval;
 *         }
 *     }
 * 
 * }
 * </PRE>
 * 
 * The print statement from the <TT>TimeKeeper</TT> gives an upper bound on how
 * far each timeslot strays from its schedule. JCSP {@link CSTimer CSTimer}s are
 * currently implemented on top of standard Java APIs (<TT>Thread.sleep</TT> and
 * <TT>Object.wait</TT>). Depending on the underlying JVM, this should stay
 * close to zero (milliseconds) - except when the rogue <TT>Worker</TT>
 * deliberately overruns a work unit. Other events may also disturb the schedule
 * - e.g. a <TT>Ctl-S</TT>/<TT>Ctl-Q</TT> from the user to
 * <I>pause</I>/<I>resume</I> output or some transient fit of activity from the
 * operating system. Some JVMs also return early from some timeouts - i.e. the
 * timeslot starts early, which gives rise to an occasional negative report from
 * the <TT>TimeKeeper</TT>.
 * <P>
 * Bear also in mind that the <TT>TimeKeeper</TT>'s print statement has to
 * compete with the print statements from all working <TT>Worker</TT>s. All are
 * scheduled to execute at the start of each timeslot and may be arbitrarilly
 * interleaved. This may be confusing when interpreting the output from the
 * system.
 * <P>
 * To clarify what's happening, we can arrange for the <TT>TimeKeeper</TT>'s
 * message to be printed first for each timeslot, <I>before</I> any from the
 * <TT>Worker</TT>s. To do this, we need to stall those <TT>Worker</TT>s
 * temporarilly until we know that the <TT>TimeKeeper</TT> has reported. A
 * simple way to do that is to double up on the barrier synchronisation. For the
 * <TT>Worker</TT>, modify its <I>working</I> loop:
 * 
 * <PRE>
 * for (int i = 0; i < nWorkUnits; i++) {
 *     barrier.sync(); // wait for everyone
 *     barrier.sync(); // wait for the Timekeeper to report
 *     System.out.println(working);
 *     tim.sleep(random.nextInt(maxWork));
 * }
 * </PRE>
 * 
 * For the <TT>TimeKeeper</TT>, modify its <TT>run</TT> loop:
 * 
 * <PRE>
 * while (true) {
 *     tim.after(timeout);
 *     barrier.sync(); // wait for everyone
 *     System.out.println("[" + (tim.read() - timeout) + "]");
 *     barrier.sync(); // let the Workers get going
 *     timeout += interval;
 * }
 * </PRE>
 * 
 * <H3>Overheads</H3> Free use of additional synchronisations to gain special
 * control (such as in the above) depends on the overheads being not so great as
 * to render that control pointless.
 * <P>
 * Going back to the <A HREF="#Worker-TimeKeeper">original example</A>, the
 * entire barrier synchronisation could be discarded by dropping the
 * <TT>TimeKeeper</TT> and making each <TT>Worker</TT> responsible for its own
 * time schedule. However, setting <TT>n</TT> timeouts (where each setting has
 * <TT>O(n)</TT> overheads) needs to be compared against setting <TT>1</TT>
 * timeout (by the <TT>TimeKeeper</TT>) together with a <TT>(n+1)</TT>-way
 * barrier synchronisation.
 * <P>
 * For the current implementation, the {@link #enroll enroll} and {@link #resign
 * resign} operations - together with <I>most</I> of the {@link #sync sync}s -
 * have unit time costs. The <I>final</I> <TT>sync</TT>, which releases all the
 * other (<TT>n</TT>) processes blocked on the barrier, takes <TT>O(n)</TT>
 * time. The unit time costs for this implementation are comparable with those
 * of a <TT>synchronized</TT> method invocation followed by an
 * <TT>Object.wait</TT>.
 * <P>
 * [<I>Note:</I> CSP synchronisation primitives can be implemented with much
 * lighter overheads. For example, the <A HREF=
 * "http://www.hensa.ac.uk/parallel/occam/projects/occam-for-all/kroc/">KRoC</A>
 * <B>occam</B> equivalent to this <TT>Barrier</TT> (its <A HREF=
 * "http://www.hensa.ac.uk/parallel/occam/projects/occam-for-all/hlps/">EVENT</A>)
 * has (sub-microsecond) unit time costs for <I>all</I> its operations,
 * including the <I>final</I> <TT>sync</TT>. Future work on JCSP may look
 * towards this standard.]
 *
 * @see AltingBarrier
 * @see Bucket
 * @see Alternative
 * 
 * @author P.H. Welch
 */

public class Barrier implements Serializable {
    private static final long serialVersionUID = 1L;
    /**
     * The number of processes currently enrolled on this barrier.
     */
    private int               nEnrolled        = 0;

    /**
     * The number of processes currently enrolled on this barrier and who have not
     * yet synchronised in this cycle.
     */
    private int countDown = 0;

    /**
     * The monitor lock used for synchronisation.
     */
    private Object barrierLock = new Object();

    /**
     * The even/odd flag used to detect spurious wakeups.
     */
    private boolean evenOddCycle = true; // could be initialised to false ...

    /**
     * Construct a barrier initially associated with no processes.
     */
    public Barrier() {
    }

    /**
     * Construct a barrier (initially) associated with <TT>nEnrolled</TT> processes.
     * It is the responsibility of the constructing process to pass this (by
     * constructor or <tt>set</tt> method) to each process that will be
     * synchronising on the barrier, <i>before</i> firing up those processes.
     * <p>
     *
     * @param nEnrolled the number of processes (initially) associated with this
     *                  barrier.
     *                  <p>
     * 
     * @throws IllegalArgumentException if <tt>nEnrolled</tt> < <tt>0</tt>.
     */
    public Barrier(final int nEnrolled) {
        if (nEnrolled < 0) {
            throw new IllegalArgumentException("*** Attempt to set a negative enrollment on a barrier\n");
        }
        this.nEnrolled = nEnrolled;
        countDown = nEnrolled;
//System.out.println ("Barrier.constructor : " + nEnrolled + ", " + countDown);
    }

    /**
     * Reset this barrier to be associated with <TT>nEnrolled</TT> processes. This
     * must only be done at a time when no processes are active on the barrier. It
     * is the responsibility of the invoking process to pass this barrier (by
     * constructor or <tt>set</tt> method) to each process that will be
     * synchronising on the barrier, <i>before</i> firing up those processes.
     * <p>
     *
     * @param nEnrolled the number of processes reset to this barrier.
     *                  <p>
     * 
     * @throws IllegalArgumentException if <tt>nEnrolled</tt> < <tt>0</tt>.
     */
    public void reset(final int nEnrolled) {
        if (nEnrolled < 0) {
            throw new IllegalArgumentException("*** Attempt to set a negative enrollment on a barrier\n");
        }
        synchronized (barrierLock) {
            this.nEnrolled = nEnrolled;
            countDown = nEnrolled;
        }
//System.out.println ("Barrier.reset : " + nEnrolled + ", " + countDown);
    }

    /**
     * Synchronise the invoking process on this barrier. <I>Any</I> process
     * synchronising on this barrier will be blocked until <I>all</I> processes
     * associated with the barrier have synchronised (or resigned).
     */
    public void sync() {
        synchronized (barrierLock) {
            countDown--;
//System.out.println ("Barrier.sync : " + nEnrolled + ", " + countDown);
            if (countDown > 0) {
                try {
                    boolean spuriousCycle = evenOddCycle;
                    barrierLock.wait();
                    while (spuriousCycle == evenOddCycle) {
                        if (Spurious.logging) {
                            SpuriousLog.record(SpuriousLog.BarrierSync);
                        }
                        barrierLock.wait();
                    }
                } catch (InterruptedException e) {
                    throw new ProcessInterruptedException("*** Thrown from Barrier.sync ()\n" + e.toString());
                }
            } else {
                countDown = nEnrolled;
                evenOddCycle = !evenOddCycle; // to detect spurious wakeups :(
//System.out.println ("Barrier.sync : " + nEnrolled + ", " + countDown);
                barrierLock.notifyAll();
            }
        }
    }

    /**
     * A process may enroll only if it is resigned. A re-enrolled process may resume
     * offering to synchronise on this barrier (until a subsequent {@link #resign
     * resign}). Other processes cannot complete the barrier (represented by this
     * front-end) without participation by the re-enrolled process.
     * <p>
     * <i>Note:</i> timing re-enrollment on a barrier usually needs some care. If
     * the barrier is being used for synchronising phases of execution between a set
     * of processes, it is crucial that re-enrollment occurs in an appropriate
     * <i>(not arbitrary)</i> phase. If the trigger for re-enrollment comes from
     * another enrolled process, that process should be in such an appropriate
     * phase. The resigned process should re-enroll and, then, acknowledge the
     * trigger. The triggering process should wait for that acknowledgement. If the
     * decision to re-enroll is internal (e.g. following a timeout), a <i>buddy</i>
     * process, enrolled on the barrier, should be asked to provide that trigger
     * when in an appropriate phase. The <i>buddy</i> process, perhaps specially
     * built just for this purpose, polls a service channel for that question when
     * in that phase.
     * </p>
     * <p>
     * <i>Warning:</i> the rule in the first sentence above is the responsibility of
     * the designer -- it is not checked by implementation. If not honoured, things
     * will go wrong.
     */
    public void enroll() {
        synchronized (barrierLock) {
            nEnrolled++;
            countDown++;
        }
//System.out.println ("Barrier.enroll : " + nEnrolled + ", " + countDown);
    }

    /**
     * A process may resign only if it is enrolled. A resigned process may not offer
     * to synchronise on this barrier (until a subsequent {@link #enroll enroll}).
     * Other processes can complete the barrier (represented by this front-end)
     * without participation by the resigned process.
     * <p>
     * Unless <i>all</i> processes synchronising on this barrier terminate in the
     * same phase, it is usually appropriate for a terminating process to
     * <i>resign</i> first. Otherwise, its sibling processes will never be able to
     * complete another synchronisation.
     * </p>
     * <p>
     * <i>Warning:</i> the rules in the first two sentences above are the
     * responsibility of the designer -- they are not checked by implementation. If
     * not honoured, things will go wrong.
     * </p>
     * <p>
     * 
     * @throws BarrierError if not enrolled <i>(but this is not always
     *                      detected)</i>.
     * 
     */
    public void resign() {
        synchronized (barrierLock) {
            nEnrolled--;
            countDown--;
//System.out.println ("Barrier.resign : " + nEnrolled + ", " + countDown);
            if (countDown == 0) {
                countDown = nEnrolled;
                evenOddCycle = !evenOddCycle; // to detect spurious wakeups :(
//System.out.println ("Barrier.resign : " + nEnrolled + ", " + countDown);
                barrierLock.notifyAll();
            } else if (countDown < 0) {
                throw new BarrierError("*** A process has resigned on a barrier without first enrolling\n");
            }
        }
    }

}
