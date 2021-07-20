
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
 * This is an extension of the {@link Parallel} class that prioritises the
 * processes given to its control.
 * <H2>Description</H2> <TT>PriParallel</TT> is an extension of the
 * {@link Parallel} class that prioritises the processes given to its control.
 * The ordering of the processes in the array passed to the constructor (or
 * added/inserted later) is significant, with earlier processes having higher
 * priority. The last process in the array inherits the priority of the
 * constructing process. That priority may be set explicitly by
 * {@link #setPriority <TT>setPriority</TT>}.
 * <P>
 * <I>Implementation Note</I>: these priorities are currently implemented using
 * the underlying threads priority mechanism. If there are more priorities
 * required than the maximum allowed for the threadgroup of the spawning
 * process, the higher requested priorities will be truncated to that maximum.
 * Also, the semantics of priority will be that implemented by the JVM being
 * used.
 *
 * @author P.D. Austin
 */

public class PriParallel extends Parallel {
    /**
     * Construct a new PriParallel object initially without any processes. Processes
     * may be added later using the inherited addProcess methods. The order of their
     * adding is significant, with ealier processes having higher priority.
     */
    public PriParallel() {
        super(null, true);
    }

    /**
     * Construct a new PriParallel object with the processes specified. The ordering
     * of the processes in the array is significant, with ealier processes having
     * higher priority. The last process in the array inherits the priority of the
     * constructing process.
     *
     * @param processes The processes to be executed in parallel
     */
    public PriParallel(CSProcess[] processes) {
        super(processes, true);
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
    @Override
    public void insertProcessAt(CSProcess process, int index) {
        super.insertProcessAt(process, index);
    }

    /**
     * This returns the current priority of this process.
     * <P>
     * 
     * @return the current priority of this process.
     */
    public static int getPriority() {
        return Thread.currentThread().getPriority();
    }

    /**
     * This changes the priority of this process. Note that JCSP only provides this
     * method for changing the priority of the <I>invoking</I> process. Changing the
     * process of <I>another</I> process is not considered wise.
     * <P>
     * <I>Implementation Note</I>: these priorities are currently implemented using
     * the underlying threads priority mechanism - hence run time exceptions
     * corresponding to the {@link Thread}.<TT>getPriority()</TT> may be thrown.
     * <P>
     * 
     * @throws <TT>java.lang.IllegalArgumentException</TT> if the priority is not in
     * the range supported by the underlying threads implementation.
     * @throws <TT>java.lang.SecurityException</TT> if the security manager of the
     * underlying threads implementation will not allow this modification.
     */
    public static void setPriority(int newPriority) {
        Thread.currentThread().setPriority(newPriority);
    }
}
