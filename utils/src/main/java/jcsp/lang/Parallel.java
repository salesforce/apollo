
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

import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

/**
 * This process constructor taks an array of <TT>CSProcess</TT>es and returns a
 * <TT>CSProcess</TT> that is the parallel composition of its process arguments.
 * <P>
 * <A HREF="#constructor_summary">Shortcut to the Constructor and Method
 * Summaries.</A>
 * <H2>Description</H2> The <TT>Parallel</TT> constructor taks an array of
 * <TT>CSProcess</TT>es and returns a <TT>CSProcess</TT> that is the parallel
 * composition of its process arguments. A <TT>run</TT> of a <TT>Parallel</TT>
 * process terminates when, and only when, all its component processes
 * terminate.
 * <P>
 * <I>Note: for those familiar with the <I><B>occam</B></I> multiprocessing
 * language, the </I><TT>Parallel</TT><I> class gives the semantics of the
 * </I><TT>PAR</TT><I> construct. However, none of the parallel usage checks
 * mandated by <I><B>occam</B></I> can be made by the Java compiler, so we need
 * to exercise that care ourselves. For instance, do not try to run the same
 * process instance more than once in parallel and, generally, watch out for
 * accidentally shared objects! Running different instances of the same process
 * in parallel is, of course, allowed.</I>
 * <P>
 * <TT>CSProcess</TT>es can be added to a <TT>Parallel</TT> object either via
 * the {@link #Parallel(CSProcess[]) constructor} or the {@link #addProcess
 * <TT>addProcess</TT>} methods. If a call to <TT>addProcess</TT> is made while
 * the <TT>run</TT> method is executing, the extra process(es) will not be
 * included in the network until the next time <TT>run</TT> is invoked.
 * <P>
 * <TT>CSProcess</TT>es can be removed from a <TT>Parallel</TT> object via the
 * {@link #removeProcess <TT>removeProcess</TT>} or {@link #removeAllProcesses
 * <TT>removeAllProcesses</TT>} method. If a call to <TT>removeProcess</TT> or
 * <TT>removeAllProcesses</TT> is made while the <TT>run</TT> method is
 * executing, the process will not be removed from the network until the next
 * time <TT>run</TT> is invoked.
 * <P>
 * <I>Note</I>: to add/remove a process to/from a network whilst it is running,
 * see the {@link ProcessManager} class.
 * </P>
 * <H2>Example</H2> The following examples demonstrate high and low level use of
 * <TT>Parallel</TT>.
 * <H3>High Level</H3> This <I>high-level</I> example sets up a communicating
 * network of (in this case non-terminating) processes. Data-flow diagrams are a
 * great help for designing, understanding and maintaining such parallel
 * systems:
 * <p>
 * <IMG SRC="doc-files\Parallel1.gif">
 * </p>
 * Here is the JCSP code:
 * 
 * <PRE>
 * import jcsp.lang.*;
 * import jcsp.plugNplay.*;
 * <I></I>
 * class ParaplexIntTest {
 * <I></I>
 *   public static void main (String[] args) {
 * <I></I>
 *     final One2OneChannelInt[] a = Channel.one2oneIntArray (3);
 *     final One2OneChannel b = Channel.one2one ();
 * <I></I>
 *     new Parallel (
 *       new CSProcess[] {
 *         new NumbersInt (a[0].out ()),
 *         new SquaresInt (a[1].out ()),
 *         new FibonacciInt (a[2].out ()),
 *         new ParaplexInt (ChannelInt.getInputArray (a), b.out ()),
 *         new CSProcess () {
 *           public void run () {
 *             System.out.println ("\n\t\tNumbers\t\tSquares\t\tFibonacci\n");
 *             while (true) {
 *               int[] data = (int[]) b.in ().read ();
 *               for (int i = 0; i < data.length; i++) {
 *                 System.out.print ("\t\t" + data[i]);
 *               }
 *               System.out.println ();
 *             }
 *           }
 *         }
 *       }
 *     ).run ();
 *   }
 * <I></I>
 * }
 * </PRE>
 * 
 * This example tabulates columns of (respectively) natural numbers, perfect
 * squares and the Fibonacci sequence. At this level, we are only aware of five
 * communicating processes: three that generate the respective sequences of
 * integers, one that multiplexes a single item from each sequence into a single
 * packet and the <I>in-lined</I> process that receives this packet and
 * tabulates its contents. And, at this level, that is all we need to think
 * about.
 * <P>
 * However, clicking on any of the generator processes reveals sub-networks
 * (and, in the case of {@link jcsp.plugNplay.ints.SquaresInt} and
 * {@link jcsp.plugNplay.ints.FibonacciInt}, sub-sub-networks). Altogether, the
 * example contains 28 parallel processes -- 18 of them <I>high-level</I> (and
 * non-terminating) and 10 <A HREF="#Low"><I>low-level</I></A> (and transient,
 * but repeatedly re-invoked). One of the key benefits of CSP is that its
 * semantics are <I>compositional</I> -- i.e. we do not have to reason about all
 * those 28 processes at the same time to reason about how they behave in this
 * application. We can build up the complexity in layers.
 * <P>
 * <I>Note:</I> the above example is just to build fluency with the
 * CSP/<B>occam</B> concept of parallel composition and to show how easy it is.
 * The network decomposes into fine-grained <I>stateless</I> components that
 * would be excellent if we were refining this application down to a silicon
 * (e.g. FPGA) implementation -- but for software running on a uni-processor
 * JVM, we would not suggest going quite so far!
 * <P>
 * <I>Note:</I> the above layered network of communicating parallel processes is
 * completely <I>deterministic</I>. It will produce the same results regardless
 * of the scheduling characteristics of the underlying JVM and regardless of its
 * physical distribution on to separate processors (and their relative speeds).
 * This default determinism is one of the founding strengths of CSP concurrency
 * that reinforces confidence in the systems we build with it.
 * <P>
 * <I>Non-determinism</I>, of course, needs to be addressed for many
 * applications and is catered for in JCSP by its {@link Alternative} construct
 * (which corresponds to the CSP external choice operator and <B>occam</B>
 * <TT>ALT</TT>), by its <I>any-1</I>, <I>1-any</I> and <I>any-any</I> channels
 * (e.g. {@link Any2OneChannel}) and by the <I>overwriting</I> semantics that
 * can be defined for its channels (e.g.
 * {@link jcsp.util.OverWriteOldestBuffer}). The fact that non-determinism has
 * to be <I>explicitly</I> introduced reduces the chance of overlooking
 * race-hazards caused by that non-determinism.
 * <H3><A NAME="Low">Low Level</H3> For a <I>low-level</I> application of
 * <TT>Parallel</TT>, here is the implementation of the
 * {@link jcsp.plugNplay.ints.ParaplexInt} process used in the <I>high-level</I>
 * example above:
 * 
 * <PRE>
 *
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

package jcsp.plugNplay.ints;
 * <I></I>
 * import jcsp.lang.*;
 * <I></I>
 * public final class ParaplexInt implements CSProcess {
 * <I></I>
 *   private final ChannelInputInt[] in;
 * <I></I>
 *   private final ChannelOutput out;
 * <I></I>
 *   public ParaplexInt (final ChannelInputInt[] in, final ChannelOutput out) {
 *     this.in = in;
 *     this.out = out;
 *   }
 * <I></I>
 *   public void run () {
 * <I></I>
 *     final ProcessReadInt[] inputProcess = new ProcessReadInt[in.length];
 *     for (int i = 0; i < in.length; i++) {
 *       inputProcess[i] = new ProcessReadInt (in[i]);
 *     }
 * <I></I>
 *     Parallel parInput = new Parallel (inputProcess);
 * <I></I>
 *     int[][] data = new int[2][in.length];               // double-buffer
 *     int index = 0;                                      // initial buffer index
 * <I></I>
 *     while (true) {
 *       parInput.run ();
 *       int[] buffer = data[index];                       // grab a buffer
 *       for (int i = 0; i < in.length; i++) {
 *         buffer[i] = inputProcess[i].value;
 *       }
 *       out.write (buffer);
 *       index = 1 - index;                                // switch buffers
 *     }
 * <I></I>
 *   }
 * <I></I>
 * }
 * </PRE>
 * 
 * Note that the <TT>Parallel</TT> object (<TT>parInput</TT>) is constructed
 * once and contains an array of processes
 * ({@link jcsp.plugNplay.ints.ProcessReadInt}), each of which performs only a
 * single channel input and, then, terminates. Each time it is run
 * (<TT>parInput.run</TT> inside the loop), all those sub-processes run
 * concurrently -- the parallel run terminating when, and only when, all those
 * sub-processes have terminated. See the documentation of
 * {@link jcsp.plugNplay.ints.ParaplexInt} for the motivation for this low-level
 * concurrency (and for the <I>double-buffering</I>).
 * </P>
 * <H2>Implementation Note</H2> The <TT>Parallel</TT> object creates new
 * {@link Thread}s to run the first <TT>(n - 1)</TT> of its processes, running
 * the last one in its own thread of control. After each <TT>run</TT> of the
 * <TT>Parallel</TT> {@link CSProcess}, all those threads are parked for reuse
 * in the next <TT>run</TT>. Thus in the above <I>low-level</I> application, the
 * overhead for Java thread creation for the internal concurrency is only
 * incurred on its first cycle. All these implementation <TT>Thread</TT>s are
 * <I>daemons</I> and, so, will terminate if everything else terminates.
 * <P>
 * If a <TT>Parallel</TT> process has finished its <TT>run()</TT> and <I>is
 * not</I> going to be used again, its parked threads may be unparked and
 * terminated by invoking its {@link #releaseAllThreads
 * <TT>releaseAllThreads</TT>} method. This will release the memory used by
 * those threads.
 *
 * @see CSProcess
 * @see ProcessManager
 * @see jcsp.lang.Sequence
 *
 * @author P.D. Austin
 * @author P.H. Welch
 */

public class Parallel implements CSProcess {
    /**
     * Monitor for internal synchronisation.
     */
    private final Object sync = new Object();

    /** The processes to be executed in <TT>Parallel</TT> */
    private CSProcess[] processes;

    /** The number of processes in this <TT>Parallel</TT> */
    private int nProcesses = 0;

    /** A pool of ParThreads */
    private ParThread[] parThreads;

    /** The number of threads created so far by this <TT>Parallel</TT> */
    private int nThreads = 0;

    // invariant : (0 <= nProcesses <= processes.length)
    // invariant : (0 <= nThreads <= parThreads.length)

    /**
     * Used to synchronise the termination of processes in each run of
     * <TT>Parallel</TT>
     */
    private Barrier barrier = new Barrier();

    private boolean priority;

    private boolean processesChanged;

    /**
     * The threads created by <I>all</I> <TT>Parallel</TT> and
     * {@link ProcessManager} objects.
     */
    private static final Set<Thread> allParThreads = Collections.synchronizedSet(new HashSet<Thread>());

    /**
     * Indicates that the <TT>destroy()</TT> method has already been called.
     */
    private static boolean destroyCalled = false;

    /**
     * Adds the thread object to the <code>allParThreads</code> collection. This
     * should be called by any infrastructure threads when they start.
     *
     * @param newThread the thread to be added to the collection.
     */
    static void addToAllParThreads(final Thread newThread) throws InterruptedException {
        synchronized (allParThreads) {
            if (destroyCalled)
                throw new InterruptedException("Parallel.destroy() has been called");
            allParThreads.add(newThread);
        }
    }

    /**
     * Removes the thread object from the <code>allParThreads</code> collection.
     */
    static void removeFromAllParThreads(final Thread oldThread) {
        synchronized (allParThreads) {
            allParThreads.remove(oldThread);
        }
    }

    /**
     * Stops all threads created by <I>all</I> <TT>Parallel</TT> and
     * {@link ProcessManager} objects. No new threads can be created until the
     * <TT>resetDestroy</TT> method gets called.
     */
    public static void destroy() {
        synchronized (allParThreads) {
            if (!destroyCalled) {
                System.out.println("*** jcsp.lang.Parallel: stopping " + allParThreads.size() + " threads");
                for (Iterator<Thread> i = allParThreads.iterator(); i.hasNext();) {
                    final Thread t = i.next();
                    try {
                        t.interrupt();
                    } catch (SecurityException e) {
                        System.out.println("*** jcsp.lang.Parallel: couldn't stop thread " + t
                        + " - security exception");
                    }
                }
                destroyCalled = true;
            }
        }
    }

    /**
     * Cancels a call to <TT>destroy</TT> allowing the JCSP system to be reused.
     * This is provided to that <TT>destroy</TT> can be called from an Applet's
     * termination method, but the Applet can be restarted later.
     */
    public static void resetDestroy() {
        synchronized (allParThreads) {
            destroyCalled = false;
        }
    }

    /**
     * Construct a new <TT>Parallel</TT> object initially without any processes.
     */
    public Parallel() {
        this(null, false);
    }

    /**
     * Construct a new <TT>Parallel</TT> object initially without any processes. If
     * the priority parameter has the value true, priorities higher in the process
     * list will be given a higher priority.
     *
     * @param priority indicates that different priorities should be given to
     *                 processes.
     */
    Parallel(boolean priority) // package visibilty
    {
        this(null, priority);
    }

    /**
     * Construct a new <TT>Parallel</TT> object with the processes specified.
     *
     * @param processes The processes to be executed in parallel
     */
    public Parallel(CSProcess[] processes) {
        this(processes, false);
    }

    /**
     * Construct a new <TT>Parallel</TT> object with the processes specified.
     *
     * @param processes The processes to be executed in parallel
     */
    public Parallel(CSProcess[][] processes) {
        if (processes != null) {
            int count = 0;
            for (int i = 0; i < processes.length; i++) {
                if (processes[i] != null)
                    count += processes[i].length;
            }
            nProcesses = count;
            this.processes = new CSProcess[nProcesses];
            count = 0;
            for (int i = 0; i < processes.length; i++) {
                if (processes[i] != null) {
                    int n = processes[i].length;
                    System.arraycopy(processes[i], 0, this.processes, count, n);
                    count += n;
                }
            }
            parThreads = new ParThread[nProcesses];
        } else {
            nProcesses = 0;
            this.processes = new CSProcess[0];
            parThreads = new ParThread[0];
        }
        processesChanged = true;
        this.priority = false;
    }

    /**
     * Construct a new <TT>Parallel</TT> object with the processes specified. If the
     * priority parameter has the value true, priorities higher in the process list
     * will be given a higher priority.
     *
     * @param processes the processes to be executed in parallel
     * @param priority  indicates that different priorities should be given to
     *                  processes.
     */
    Parallel(CSProcess[] processes, boolean priority) // package visibilty
    {
        if (processes != null) {
            nProcesses = processes.length;
            this.processes = new CSProcess[nProcesses];
            System.arraycopy(processes, 0, this.processes, 0, nProcesses);
            parThreads = new ParThread[nProcesses];
        } else {
            nProcesses = 0;
            this.processes = new CSProcess[0];
            parThreads = new ParThread[0];
        }
        processesChanged = true;
        this.priority = priority;
    }

    /**
     * Add the process to the <TT>Parallel</TT> object. The extended network will be
     * executed the next time <TT>run()</TT> is invoked.
     *
     * @param process the CSProcess to be added
     */
    public void addProcess(CSProcess process) {
        synchronized (sync) {
            if (process != null) {
                final int targetProcesses = nProcesses + 1;
                if (targetProcesses > processes.length) {
                    final CSProcess[] tmp = processes;
                    processes = new CSProcess[2 * targetProcesses];
                    System.arraycopy(tmp, 0, processes, 0, nProcesses);
                }
                processes[nProcesses] = process;
                nProcesses = targetProcesses;
                processesChanged = true;
            }
        }
    }

    /**
     * Add the array of processes to the <TT>Parallel</TT> object. The extended
     * network will be executed the next time <TT>run()</TT> is invoked.
     *
     * @param newProcesses the CSProcesses to be added
     */
    public void addProcess(CSProcess[] newProcesses) {
        synchronized (sync) {
            if (processes != null) {
                final int extra = newProcesses.length;
                final int targetProcesses = nProcesses + extra;
                if (targetProcesses > processes.length) {
                    final CSProcess[] tmp = processes;
                    processes = new CSProcess[2 * targetProcesses];
                    System.arraycopy(tmp, 0, processes, 0, nProcesses);
                }
                System.arraycopy(newProcesses, 0, processes, nProcesses, extra);
                nProcesses = targetProcesses;
                processesChanged = true;
            }
        }
    }

    /**
     * Insert another process to the pri-parallel object at the specifed index. The
     * point of insertion is significant because the ordering of process components
     * determines the priorities. The extended network will be executed the next
     * time run() is invoked.
     * <P>
     * 
     * @param process the process to be inserted
     * @param index   the index at which to insert the process
     */
    public void insertProcessAt(CSProcess process, int index) {
        synchronized (sync) {
            if (index >= nProcesses + 1)
                throw new ArrayIndexOutOfBoundsException(index + " > " + (nProcesses + 1));
            if (process != null) {
                final int targetProcesses = nProcesses + 1;
                if (targetProcesses > processes.length) {
                    final CSProcess[] tmp = processes;
                    processes = new CSProcess[2 * targetProcesses];
                    System.arraycopy(tmp, 0, processes, 0, index);
                    System.arraycopy(tmp, index, processes, index + 1, nProcesses - index);
                } else {
                    if (index < nProcesses)
                        System.arraycopy(processes, index, processes, index + 1, nProcesses - index);
                }
                processes[index] = process;
                nProcesses = targetProcesses;
                processesChanged = true;
            }
        }
    }

    /**
     * Remove the process from the <TT>Parallel</TT> object. The cut-down network
     * will not be executed until the next time <TT>run()</TT> is invoked.
     *
     * @param process the CSProcess to be removed
     */
    public void removeProcess(CSProcess process) {
        synchronized (sync) {
            for (int i = 0; i < nProcesses; i++) {
                if (processes[i] == process) {
                    if (i < nProcesses - 1)
                        System.arraycopy(processes, i + 1, processes, i, nProcesses - (i + 1));
                    nProcesses--;
                    processes[nProcesses] = null;
                    processesChanged = true;
                    return;
                }
            }
        }
    }

    /**
     * Remove all processes from the <TT>Parallel</TT> object. The cut-down network
     * will not be executed until the next time <TT>run()</TT> is invoked.
     */
    public void removeAllProcesses() {
        synchronized (sync) {
            for (int i = 0; i < nProcesses; i++) {
                processes[i] = null;
            }
            nProcesses = 0;
            processesChanged = true;
        }
    }

    /**
     * System finalizer. When this object falls out of scope it will release all of
     * the threads that it has allocated.
     */
    @Override
    protected void finalize() throws Throwable {
        releaseAllThreads();
    }

    /**
     * Release all threads saved by the <TT>Parallel</TT> object for future runs -
     * the threads all terminate and release their associated workspaces. This
     * should only be executed when the <TT>Parallel</TT> object is not running. If
     * this <TT>Parallel</TT> object is run again, the necessary threads will be
     * recreated.
     */
    public void releaseAllThreads() {
        synchronized (sync) {
            for (int i = 0; i < nThreads; i++) {
                parThreads[i].terminate();
                parThreads[i] = null;
            }
            nThreads = 0;
            processesChanged = true;
        }
    }

    /**
     * @return the number of processes currently registered.
     */
    public int getNumberProcesses() {
        int n;
        synchronized (sync) {
            n = nProcesses;
        }
        return n;
    }

    /**
     * Run the parallel composition of the processes registered with this
     * <TT>Parallel</TT> object. It terminates when, and only when, all its
     * component processes terminate.
     * </P>
     * <P>
     * <I>Implementation note: In its first </I>run<I>, only (numProcesses - 1)
     * Threads are created to run the processes -- the last process is executed in
     * the invoking Thread. Sunsequent </I>run<I>s reuse these Threads (so the
     * overhead of thread creation happens only once).</I>
     * </P>
     */
    @Override
    public void run() {

        boolean emptyRun = true;

        CSProcess myProcess = null;

        synchronized (sync) {

            if (nProcesses > 0) {

                emptyRun = false;

                int currentPriority = 0;
                int maxPriority = 0;
                if (priority) {
                    Thread thread = Thread.currentThread();
                    currentPriority = thread.getPriority();
                    maxPriority = Math.min(currentPriority + nProcesses - 1,
                                           Math.min(Thread.MAX_PRIORITY, thread.getThreadGroup().getMaxPriority()));
                }

                barrier.reset(nProcesses);
                myProcess = processes[nProcesses - 1];
                if (processesChanged) {
                    if (nThreads < nProcesses - 1) {
                        if (parThreads.length < nProcesses - 1) {
                            final ParThread[] tmp = parThreads;
                            parThreads = new ParThread[processes.length];
                            System.arraycopy(tmp, 0, parThreads, 0, nThreads);
                        }
                        for (int i = 0; i < nThreads; i++) {
                            parThreads[i].reset(processes[i], barrier);
                            if (priority) {
                                parThreads[i].setPriority(Math.max(currentPriority, maxPriority - i));
                            }
                            parThreads[i].release();
                        }
                        for (int i = nThreads; i < nProcesses - 1; i++) {
                            parThreads[i] = new ParThread(processes[i], barrier);
                            if (priority) {
                                parThreads[i].setPriority(Math.max(currentPriority, maxPriority - i));
                            }
                            parThreads[i].start();
                        }
                        nThreads = nProcesses - 1;
                    } else {
                        for (int i = 0; i < nProcesses - 1; i++) {
                            parThreads[i].reset(processes[i], barrier);
                            if (priority) {
                                parThreads[i].setPriority(Math.max(currentPriority, maxPriority - i));
                            }
                            parThreads[i].release();
                        }
                    }
                    processesChanged = false;
                } else {
                    for (int i = 0; i < nProcesses - 1; i++) {
                        if (priority) {
                            parThreads[i].setPriority(Math.max(currentPriority, maxPriority - i));
                        }
                        parThreads[i].release();
                    }
                }
            }

        } // end synchronized block

        if (!emptyRun) {

            try {
                myProcess.run();
            } catch (ProcessInterruptedException e) {
                // If this was raised then we must propogate the interrupt signal to other
                // processes
                // PHW: Why? This seems unnecessary ... and, in any case, isn't done if sibling
                // processes (in their parThreads) are interrupted!
                synchronized (sync) { // PHW: why synchronize?
                    for (int i = 0; i < nThreads; i++) {
                        try {
                            parThreads[i].interrupt();
                        } catch (Throwable t) {
                            System.out.println("*** jcsp.lang.Parallel: couldn't stop thread " + t
                            + " - security exception");
                        }
                    }
                }
            } catch (Throwable e) {
                uncaughtException("jcsp.lang.Parallel", e);
            }

            barrier.sync();

        }
    }

    /**
     * TRUE iff uncaught exceptions are to be displayed.
     */
    private static boolean displayExceptions = true;

    /**
     * TRUE iff uncaught errors are to the displayed.
     */
    private static boolean displayErrors = true;

    /**
     * Enables or disables the display of Exceptions uncaught by a CSProcess running
     * within a Parallel or under a ProcessManager object.
     */
    public static void setUncaughtExceptionDisplay(final boolean enable) {
        displayExceptions = enable;
    }

    /**
     * Enables or disables the display or Errors uncaught by a CSProcess running
     * within a Parallel or under a ProcessManager object.
     */
    public static void setUncaughtErrorDisplay(final boolean enable) {
        displayErrors = enable;
    }

    static void uncaughtException(final String caller, final Throwable t) {
        if (((t instanceof Error) && displayErrors) || ((t instanceof Exception) && displayExceptions)) {
            synchronized (System.err) {
                System.err.println("\n*** " + caller + ": A process threw the following exception :");
                t.printStackTrace();
                System.err.println();
            }
        }
    }
}
