
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
 * This constructor taks an array of <TT>CSProcess</TT>es and returns a
 * <TT>CSProcess</TT> that is the sequential composition of its process
 * arguments.
 * <P>
 * <A HREF="#constructor_summary">Shortcut to the Constructor and Method
 * Summaries.</A>
 * <H2>Description</H2> The <TT>Sequence</TT> constructor taks an array of
 * <TT>CSProcess</TT>es and returns a <TT>CSProcess</TT> that is the sequential
 * composition of its process arguments.
 * <P>
 * <I>Note: for those familiar with the <I><B>occam</B></I> programming
 * language, the <TT>Sequence</TT> class gives the semantics of the <TT>SEQ</TT>
 * construct.</I>
 * <P>
 * This class is included for completeness. Sequential code is normally be
 * handled by sequential Java code. If we need to run some <TT>CSProcess</TT>es
 * in sequence, we can just execute their <TT>run</TT> methods in sequence.
 * However, using this <TT>Sequence</TT> class lets us switch between sequential
 * and parallel composition of processes with a single word change (replace
 * <TT>Sequence</TT> by <TT>Parallel</TT>) in our code. For some applications
 * (e.g. the example below), this may provide a useful comparison.
 * <P>
 * <TT>CSProcess</TT>es can be added to a <TT>Sequence</TT> object either via
 * the {@link #Sequence(CSProcess[]) constructor} or the {@link #addProcess
 * <TT>addProcess</TT>} methods. If a call to <TT>addProcess</TT> is made while
 * the <TT>run</TT> method is executing, the extra process(es) will not be
 * executed until the next time <TT>run</TT> is invoked.
 * <P>
 * <TT>CSProcess</TT>es can be removed from <TT>Sequence</TT> object via the
 * {@link #removeProcess <TT>removeProcess</TT>} or {@link #removeAllProcesses
 * <TT>removeAllProcesses</TT>} method. If a call to <TT>removeProcess</TT> or
 * <TT>removeAllProcesses</TT> is made while the <TT>run</TT> method is
 * executing, the process will not be removed from the network until the next
 * time <TT>run</TT> is invoked.
 * </P>
 * <H2>Example - Matrix Multiply (in Sequence and in Parallel)</H2>
 * <P>
 * The following example illustrates a simple transformation of sequential code
 * into parallel code. On a shared-memory parallel machine (e.g. a Symmetric
 * Multi-Processor) and depending on the overheads for starting up and shutting
 * down the concurrency and the scale of the problem to which it is applied, the
 * parallel code will terminate earlier than its sequential equivalent.
 *
 * <H3><A NAME="multiply">Sequential Implementation</H3> The example is simple
 * matrix multiplication. The standard implementation consists of three nested
 * <TT>for</TT>-loops, the innermost of which computes the <I>dot-product</I> of
 * a row (of the first matrix) against a column (of the second) to produce one
 * element of the result matrix. It is expressed here as a static method, which
 * would belong to some larger collection of static methods providing a general
 * matrix handling class:
 * 
 * <PRE>
 *   public static void multiply (final double[][] X,
 *                                final double[][] Y,
 *                                final double[][] Z) {
 * <I></I>
 *     ...  check (X[0].length == Y.length)
 *     ...  check (X.length == Z.length)
 *     ...  check (Y[0].length == Z[0].length)
 * <I></I>
 *     for (int i = 0; i < X.length; i++) {
 *       final double[] Xi = X[i];
 *       final double[] Zi = Z[i];
 *       for (int j = 0; j < Y[0].length; j++) {
 *         double sum = 0.0d;
 *         for (int k = 0; k < Y.length; k++) {
 *           sum += Xi[k]*Y[k][j];
 *         }
 *         Zi[j] = sum;
 *       }
 *     }
 * <I></I>
 *   }
 * </PRE>
 * 
 * The method computes the matrix multiplication of <TT>X</TT> by <TT>Y</TT>,
 * leaving the result in <TT>Z</TT>. The (hidden) checks ensure that the
 * supplied arrays have compatible dimensions for multiplication, throwing a
 * {@link RuntimeException <TT>java.lang.RuntimeException</TT>} if they are not.
 * The <I>abbreviations</I> <TT>Xi</TT> and <TT>Zi</TT> are not strictly
 * necessary, but for those brought up in an <B>occam</B> world are automatic
 * good practice - the <TT>Xi[k]</TT> reference in the
 * O(n<sup><font size="-2">3</font></sup>)-innermost loop saves an array index
 * computation and check.
 *
 * <H3><A NAME="parMultiply">Parallel Implementation</H3> The two outer
 * <TT>for</TT>-loops of <A HREF="#multiply"><TT>multiply</TT></A> set up the
 * particular row and column pairs for the <I>dot-product</I> computed by the
 * innermost loop. Unnecessarilly, they impose an ordering in which those
 * <I>dot-products</I> are computed. Each <I>dot-product</I> computes one
 * element of the result matrix and is independent of its sibling
 * <I>dot-products</I>. The sequential constraint may, therefore, be removed and
 * all elements of the result matrix may be computed in parallel.
 * <P>
 * If this were <B>occam</B>, all we would need to do is replace the two outer
 * <TT>for</TT>-loops with <TT>par</TT>-loops. Since this is Java, we have to
 * jump through some slightly higher syntactic hoops. First, we need to
 * transform the body of the <TT>for</TT>-loop into a <I>process</I> and use the
 * loop to assign each one to consecutive elements of a <I>process</I> array.
 * Then, we simply run the array in parallel:
 * 
 * <PRE>
 *   public static void parMultiply (final double[][] X,
 *                                   final double[][] Y,
 *                                   final double[][] Z) {
 * <I></I>
 *     ...  check array dimensions are compatible
 * <I></I>
 *     final CSProcess[] rowProcess = new CSProcess[X.length];
 * <I></I>
 *     for (int i = 0; i < X.length; i++) {
 *       final int ii = i;
 *       rowProcess[ii] = new CSProcess () {
 *         public void run () {
 *           final double[] Xi = X[ii];
 *           final double[] Zi = Z[ii];
 *           final double[][] YY = Y;
 *           for (int j = 0; j < YY[0].length; j++) {
 *             double sum = 0.0d;
 *             for (int k = 0; k < YY.length; k++) {
 *               sum += Xi[k]*YY[k][j];
 *             }
 *             Zi[j] = sum;
 *           }
 *         }
 *       };
 *     }
 * <I></I>
 *     new Parallel (rowProcess).run ();
 * <I></I>
 *   }
 * </PRE>
 * 
 * [<I>Note:</I> anonymous inner classes, as in the above code, may only access
 * <I>global</I> variables that are qualified as <TT>final</TT> - hence, the
 * need to freeze the loop control index, <TT>i</TT>, as <TT>ii</TT>. The
 * abreviation <TT>YY</TT>, of <TT>Y</TT>, is so that access to that matrix is
 * via a variable <I>local</I> to the process. The <I>global</I> <TT>Y</TT>
 * could have been used, but at the cost of needless run-time overhead.]
 *
 * <H3>Back to Sequential Implementation</H3> Having expressed this algorithm in
 * terms of <I>processes</I>, we come to the motivation for this example. To run
 * a collection of processes, we must decide whether to run them is
 * <I>parallel</I> or <I>sequence</I>. JCSP makes either decision equally easy
 * to implement. The above coding ran them in parallel. To revert to running
 * them is sequence, all we need do is change the last line of executable code
 * to:
 * 
 * <PRE>
 * new Sequence(rowProcess).run();
 * </PRE>
 * 
 * It would probably be good to name this new method <TT>seqMultiply</TT>
 * (instead or <TT>parMultiply</TT>!). Of course, it has the same behaviour as
 * the original sequential <TT>multiply</TT> and suffers only slightly increased
 * overheads - negligible for matrices above a modest size.
 *
 * <H3><TT>Sequence</TT> versus <TT>Parallel</TT></H3> Beware that it is only in
 * certain circumstances that <TT>Sequence</TT> and <TT>Parallel</TT> can be
 * interchanged without upsetting the semantics of the system. The processes
 * under their control must be finite (i.e. they must all terminate), they must
 * engage in no synchronisations (e.g. channel communication) either between
 * themselves or with other processes and they must obey CREW-parallel usage
 * rules (i.e. if one process updates some data, no other process may look at
 * it). These are fulfilled by the <TT>rowProcess</TT> array, each of whose
 * processes computes a separate row (<TT>Zi</TT>) of the target result matrix
 * (<TT>Z</TT>).
 *
 * <H3>Exercise</H3> The above <TT>parMultiply</TT> has transformed only the
 * outermost <TT>for</TT>-loop of <TT>multiply</TT> into a <TT>par</TT>-loop.
 * Current SMPs do not scale well above 4 or 8 processors, so we do not need
 * very large matrices for this code to demonstrate high parallel efficiencies.
 * Future architectures, however, may offer much higher levels of physical
 * concurrency that efficiently support much finer granularities of process. For
 * those machines and as an interesting exercise, parallelise the middle
 * <TT>for</TT>-loop. To exploit even finer levels of parallel granularity,
 * parallelise the innermost <TT>for</TT>-loop so that it computes its
 * <I>dot-product</I> in O(log(n)) time - rather than the O(n) time of the
 * sequential code.
 *
 * <H3>Two Important Modifications on <TT>parMultiply</TT></H3> The first time
 * it is <TT>run</TT>, the JCSP {@link Parallel} process creates a new
 * {@link Thread} for all but one of its component processes, running its last
 * component in the thread invoking the <TT>run</TT>. When (and if) all those
 * component processes terminate, the <TT>Parallel</TT> <TT>run</TT> terminates
 * but leaves the newly created threads parked for later use. Subsequent
 * <TT>run</TT>s of the <TT>Parallel</TT> process resuse those parked threads.
 * So the overhead of thread creation occurs only for the <I>first</I> run.
 * <P>
 * In <A HREF="#parMultiply"><TT>parMultiply</TT></A>, however, the
 * <TT>Parallel</TT> process is anonymous and local to the method and so can
 * never be used again. Worse, the threads it created and parked are left parked
 * - wasting space for the remainder of the program. Repeated invocations of
 * <TT>parMultiply</TT>, therefore, will leak memory.
 * <P>
 * There are two ways to fix this. The first is to accept that the
 * <TT>Parallel</TT> process created by <TT>parMultiply</TT> remains local to it
 * (and, hence, un-reusable) and, explicitly, to unpark and terminate its
 * unwanted threads. This can be done by invoking
 * {@link Parallel#releaseAllThreads <TT>releaseAllThreads</TT>} on the
 * <TT>Parallel</TT> after it has been <TT>run</TT> - the memory associated with
 * those threads will be released. A temporary name for the process needs to be
 * assigned and the last executable line of
 * <A HREF="#parMultiply"><TT>parMultiply</TT></A> becomes the three lines:
 * 
 * <PRE>
 * final Parallel par = new Parallel(rowProcess);
 * par.run();
 * par.releaseAllThreads();
 * </PRE>
 *
 * The second way to improve things is to save the <TT>Parallel</TT> process for
 * later runs. There is no easy way for doing this local to the class to which
 * <TT>parMultiply</TT> belongs. <TT>parMultiply</TT> is <TT>static</TT> and may
 * be invoked with matrix parameters of different dimensions. So, a
 * <TT>static</TT> data structure would need to be set up and consulted each
 * time <TT>parMultiply</TT> was invoked to see if a suitable <TT>Parallel</TT>
 * process had already been saved.
 * <P>
 * Much better is to give the <I>user</I> of <TT>parMultiply</TT> the
 * responsibilty of looking after - and directly using, reusing and (if
 * necessary) terminating the threads in - the <TT>Parallel</TT> process. So, we
 * change the method into something that <I>manufactures</I> a process that does
 * parallel matrix multiplication. That multiplication will be specific to the
 * three matrices given as parameters (i.e. it performs <TT>Z = X*Y</TT> <I>on
 * those matrices only</I>):
 * 
 * <PRE>
 *   public static Parallel makeParMultiply (final double[][] X,
 *                                           final double[][] Y,
 *                                           final double[][] Z) {
 * <I></I>
 *     ...  check array dimensions are compatible                     // as before
 * <I></I>
 *     final CSProcess[] rowProcess = new CSProcess[X.length];        // as before
 * <I></I>
 *     for (int i = 0; i < X.length; i++) {                           // as before
 *       final int ii = i;                                            // as before
 *       rowProcess[ii] = new CSProcess () {                          // as before
 *         public void run () {                                       // as before
 *           final double[] Xi = X[ii];                               // as before
 *           final double[] Zi = Z[ii];                               // as before
 *           final double[][] YY = Y;                                 // as before
 *           for (int j = 0; j < YY[0].length; j++) {                 // as before
 *             double sum = 0.0d;                                     // as before
 *             for (int k = 0; k < YY.length; k++) {                  // as before
 *               sum += Xi[k]*YY[k][j];                               // as before
 *             }                                                      // as before
 *             Zi[j] = sum;                                           // as before
 *           }                                                        // as before
 *         }                                                          // as before
 *       };                                                           // as before
 *     }                                                              // as before
 * <I></I>
 *     return new Parallel (rowProcess);
 * <I></I>
 *   }
 * </PRE>
 * 
 * The invoker of <TT>makeParMultiply</TT> gets back a parallel process that can
 * be invoked as often as needed and incurs a thread creation overhead only on
 * its first use. The data for the matrix parameters may be freely changed
 * between invocations - each invocation will work on whatever data is present
 * at the time. All the user must remember is that the manufactured parallel
 * process is good only for working with the matrix objects specified originally
 * to <TT>makeParMultiply</TT> as its parameters. For example, if
 * <TT>Matrix</TT> is the class to which <TT>makeParMultiply</TT> belongs:
 * 
 * <PRE>
 *     final Parallel par = Matrix.makeParMultiply (X, Y, Z);
 * <I></I>
 *     while (...) {
 *       ...  set up matrices X and Y
 *       par.run ();
 *       ...  do something with the result matrix Z
 *     }
 * <I></I>
 *     par.releaseAllThreads ();
 * </PRE>
 *
 *
 * @see CSProcess
 * @see Parallel
 *
 * @author P.D. Austin
 * @author P.H. Welch
 */

public class Sequence implements CSProcess {
    /** The processes to be executed in sequence */
    private CSProcess[] processes;

    /** The number of processes in this <TT>Sequence</TT> */
    private int nProcesses = 0;

    // invariant : (0 <= nProcesses <= processes.length)

    /**
     * Construct a new <TT>Sequence</TT> object initially without any processes.
     */
    public Sequence() {
        this(null);
    }

    /**
     * Construct a new <TT>Sequence</TT> object with the processes specified.
     *
     * @param processes the processes to be executed in sequence
     */
    public Sequence(CSProcess[] processes) {
        if (processes != null) {
            nProcesses = processes.length;
            this.processes = new CSProcess[nProcesses];
            System.arraycopy(processes, 0, this.processes, 0, nProcesses);
        } else {
            nProcesses = 0;
            this.processes = new CSProcess[0];
        }
    }

    /**
     * Add the process to the <TT>Sequence</TT> object. The extended network will be
     * executed the next time run() is invoked.
     *
     * @param process The CSProcess to be added
     */
    public synchronized void addProcess(CSProcess process) {
        if (process != null) {
            final int targetProcesses = nProcesses + 1;
            if (targetProcesses > processes.length) {
                final CSProcess[] tmp = processes;
                processes = new CSProcess[2 * targetProcesses];
                System.arraycopy(tmp, 0, processes, 0, nProcesses);
            }
            processes[nProcesses] = process;
            nProcesses = targetProcesses;
        }
    }

    /**
     * Add the array of processes to the <TT>Sequence</TT> object. The extended
     * network will be executed the next time run() is invoked.
     *
     * @param newProcesses the processes to be added
     */
    public synchronized void addProcess(CSProcess[] newProcesses) {
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
        }
    }

    /**
     * Remove a process from the <TT>Sequence</TT> object. The cut-down network will
     * be executed the next time run() is invoked.
     *
     * @param process the process to be removed
     */
    public synchronized void removeProcess(CSProcess process) {
        for (int i = 0; i < nProcesses; i++) {
            if (processes[i] == process) {
                if (i < nProcesses - 1)
                    System.arraycopy(processes, i + 1, processes, i, nProcesses - (i + 1));
                nProcesses--;
                processes[nProcesses] = null;
                return;
            }
        }
    }

    /**
     * Remove all processes from the <TT>Sequence</TT> object. The cut-down network
     * will not be executed until the next time <TT>run()</TT> is invoked.
     */
    public synchronized void removeAllProcesses() {
        for (int i = 0; i < nProcesses; i++)
            processes[i] = null;
        nProcesses = 0;
    }

    /**
     * Run the sequential composition of the processes registered with this
     * <TT>Sequence</TT> object.
     */
    @Override
    public void run() {
        for (int i = 0; i < nProcesses; i++)
            processes[i].run();
    }
}
