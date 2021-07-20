
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
 * This enables a {@link CSProcess} to be spawned <I>concurrently</I> with the
 * process doing the spawning.
 * <P>
 * <A HREF="#constructor_summary">Shortcut to the Constructor and Method
 * Summaries.</A>
 *
 * <H2>Description</H2> The <TT>ProcessManager</TT> class enables a
 * {@link CSProcess} to be spawned <I>concurrently</I> with the process doing
 * the spawning. The class provides methods to manage the spawned process:
 * {@link #start start}, {@link #join join} and {@link #stop stop}. The spawned
 * process may, of course, be a {@link Parallel} network of processes to any
 * depth of nesting, in which case the <I>whole</I> network comes under this
 * management.
 * <P>
 * Spawning processes is not the normal way of creating a network in JCSP - the
 * normal method is to use the {@link Parallel} class. However, when we need to
 * add processes in response to some run-time event, this capability is very
 * useful.
 * <P>
 * For completeness, <TT>ProcessManager</TT> is itself a <TT>CSProcess</TT> -
 * {@link #run run}ning a <TT>ProcessManager</TT> simply runs the process it is
 * managing.
 * </P>
 *
 * <H3>Spawning a CSProcess</H3>
 *
 * This example demonstrates that the managed <TT>CSProcess</TT> is executed
 * concurrently with the spawning process and that it dies when its manager
 * terminates. The managed process is `infinite' and just counts and chatters.
 * The managed process is automatically terminated if the main Java thread
 * terminates (as in the case, eventually, below).
 * 
 * <PRE>
 * import jcsp.lang.*;
 * <I></I>
 * public class ProcessManagerExample1 {
 * <I></I>
 *   public static void main (String[] argv) {
 * <I></I>
 *     final ProcessManager manager = new ProcessManager (
 *       new CSProcess () {
 *         public void run () {
 *           final CSTimer tim = new CSTimer ();
 *           long timeout = tim.read ();
 *           int count = 0;
 *           while (true) {
 *             System.out.println (count + " :-) managed process running ...");
 *             count++;
 *             timeout += 100;
 *             tim.after (timeout);   // every 1/10th of a second ...
 *           }
 *         }
 *       }
 *     );
 * <I></I>
 *     final CSTimer tim = new CSTimer ();
 *     long timeout = tim.read ();
 * <I></I>
 *     System.out.println ("\n\n\t\t\t\t\t
 *                         *** start the managed process");
 *     manager.start ();
 * <I></I>
 *     for (int i = 0; i < 10; i++) {
 *       System.out.println ("\n\n\t\t\t\t\t
 *                           *** I'm still executing as well");
 *       timeout += 1000;
 *       tim.after (timeout);         // every second ...
 *     }
 * <I></I>
 *     System.out.println ("\n\n\t\t\t\t\t
 *                         *** I'm finishing now!");
 *   }
 * }
 * </PRE>
 *
 * <H3>Stopping, Interrupting, Race-Hazards and Poison</H3>
 *
 * Stopping a Java thread releases any locks it (or any sub-process) may be
 * holding, so this reduces the danger of other threads deadlocking through a
 * failure to acquire a needed lock. However, if the stopped process were in the
 * middle of some synchronised transaction, the data update may be incomplete
 * (and, hence, corrupt) depending on the precise moment of the stopping. This
 * is a race-hazard. Further, if some other thread later needed to interact with
 * the stopped thread, it would deadlock.
 * <P>
 * Instead of <i>stopping</i> a JCSP process, it is much safer to
 * {@link #interrupt() <i>interrupt</i>} it. This gives the process the chance
 * to notice the interrupt (through an exception handler) and tidy up. If no
 * such handler is provided and the JCSP process attempts any synchronisation
 * afterwards, the process will bomb out with a
 * {@link ProcessInterruptedException}.
 * <P>
 * For historical reasons, a {@link #stop()} method is provided below &ndash;
 * but it is implemented as {@link #interrupt()} (and deprecated).
 * <P>
 * If the managed process has gone parallel, managing an interrupt to achieve a
 * clean exit is more tricky. Stopping a network by setting a global
 * <TT>volatile</TT> flag that each process polls from time to time is not safe.
 * For example, a thread blocked on a monitor <TT>wait</TT> will remain blocked
 * if the thread that was going to <TT>notify</TT> it spots the shut-down flag
 * and terminates.
 * <P>
 * For JCSP processes, <I>there is</I> a general solution to this [<I>`Graceful
 * Termination and Graceful Resetting'</I>, P.H.Welch, Proceedings of OUG-10,
 * pp. 310-317, Ed. A.W.P.Bakkers, IOS Press (Amsterdam), ISBN 90 5199 011 1,
 * April, 1989], based on the careful distribution of <I>poison</I> over the
 * network's normal communication channels.
 * <P>
 * However, JCSP now supports graceful termination of process networks and
 * sub-networks through a notion of <i>poisoning</i> synchoronisation objects
 * (e.g. channels) &ndash; see {@link Poisonable}.
 *
 * @see CSProcess
 * @see Parallel
 * @see jcsp.awt.ActiveApplet
 * @see Poisonable
 * @see jcsp.lang.PoisonException
 *
 * @author P.H. Welch
 * @author P.D. Austin
 */

public class ProcessManager implements CSProcess {
    /**
     * The maximum priority value for running a process.
     */
    public static final int PRIORITY_MAX = Thread.MAX_PRIORITY;

    /**
     * The normal priority value for running a process.
     */
    public static final int PRIORITY_NORM = Thread.NORM_PRIORITY;

    /**
     * The minimum priority value for running a process.
     */
    public static final int PRIORITY_MIN = Thread.MIN_PRIORITY;

    /** The CSProcess to be executed by this ProcessManager */
    private final CSProcess process;

    /** The thread supporting the CSProcess being executed by this ProcessManager */
    private Thread thread;

    /**
     * @param proc the {@link CSProcess} to be executed by this ProcessManager
     */
    public ProcessManager(CSProcess proc) {
        this.process = proc;
        thread = new Thread() {
            @Override
            public void run() {
                try {
                    Parallel.addToAllParThreads(this);
                    process.run();
                } catch (Throwable e) {
                    Parallel.uncaughtException("jcsp.lang.ProcessManager", e);
                } finally {
                    Parallel.removeFromAllParThreads(this);
                }
            }
        };
        thread.setDaemon(true);
    }

    // }}}

    // {{{ public void start ()
    /**
     * Start the managed process (but keep running ourselves).
     */
    public void start() {
        thread.start();
    }

    /**
     * Start the managed process at a specified priority (but keep running
     * ourselves). The priority of the <code>ProcessManager</code> that this is
     * called upon will remain at the specified priority once the process has
     * terminated.
     *
     * The priority should be specified as an <code>int</code> between
     * <code>PRIORITY_MIN<code> and <code>PRIORITY_MAX<code>.
     *
     * @param priority the priority at which to start the process.
     */
    public void start(int priority) {
        thread.setPriority(priority);
        start();
    }

    /**
     * Stop (permanently) the managed process.
     * 
     * This method now calls interrupt(), which will not always stop the process.
     * 
     * @deprecated
     */
    @Deprecated
    public void stop() {
        interrupt();
    }

    /**
     * Interrupt the managed process. This will usually cause the process to throw a
     * {@link ProcessInterruptedException}, which will likely halt the process.
     */
    public void interrupt() {
        thread.interrupt();
    }

    /**
     * Join the managed process (that is wait for it to terminate).
     */
    public void join() {
        try {
            thread.join();
        } catch (InterruptedException e) {
            throw new ProcessInterruptedException("Joining process " + process);
        }
    }

    /**
     * <p>
     * Run the managed process (that is start it and wait for it to terminate). This
     * will adjust the priority of the calling process to the priority of this
     * <code>ProcessManager</code> and then return the priority to the previous
     * value once the managed process has terminated.
     * </p>
     *
     * <p>
     * The managed process can be run at the caller's priority simply by directly
     * calling the <code>CSProcess</code> object's <code>run()</code> method.
     * </p>
     */
    @Override
    public void run() {
        int oldPriority = Thread.currentThread().getPriority();
        Thread.currentThread().setPriority(thread.getPriority());
        process.run();
        Thread.currentThread().setPriority(oldPriority);
    }

    /**
     * <p>
     * Public mutator for setting the <code>ProcessManager</code> object's process'
     * priority.
     * </p>
     * <p>
     * The priority should be specified as an <code>int</code> between
     * <code>PRIORITY_MIN<code> and <code>PRIORITY_MAX<code>.
     * </p>
     *
     * @param priority the priority to use.
     */
    public void setPriority(int priority) {
        thread.setPriority(priority);
    }

    /**
     * <p>
     * Public accessor for obtaining the <code>ProcessManager</code> object's
     * process' priority.
     * </p>
     *
     * @return the priority at which the <code>ProcessManager</code> object's
     *         process will be run.
     */
    public int getPriority() {
        return thread.getPriority();
    }
}
