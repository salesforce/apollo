
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
 * This is the <TT>Thread</TT> class used by {@link Parallel} to run all but one
 * of its given processes.
 *
 * <H2>Description</H2> A <TT>ParThread</TT> is a <TT>Thread</TT> used by
 * {@link Parallel} to run all but one of its given processes.
 * <P>
 * The <TT>CSProcess</TT> to be executed can be changed using the
 * <TT>setProcess</TT> method providing the <TT>ParThread</TT> is not active.
 *
 * @see CSProcess
 * @see jcsp.lang.ProcessManager
 * @see Parallel
 *
 * @author P.D. Austin
 * @author P.H. Welch
 */
//}}}

class ParThread extends Thread {
    /** the process to be executed */
    private CSProcess process;

    /** the barrier at the end of a PAR */
    private Barrier barrier;

    private boolean running = true;

    /** parking barrier for this thread */
    private Barrier park = new Barrier(2);

    /**
     * Construct a new ParThread.
     *
     * @param process the process to be executed
     * @param barrier the barrier for then end of the PAR
     */
    public ParThread(CSProcess process, Barrier barrier) {
        setDaemon(true);
        this.process = process;
        this.barrier = barrier;
        setName(process.toString());
    }

    /**
     * reset the ParThread.
     *
     * @param process the process to be executed
     * @param barrier the barrier for then end of the PAR
     */
    public void reset(CSProcess process, Barrier barrier) {
        this.process = process;
        this.barrier = barrier;
        setName(process.toString());
    }

    /**
     * Sets the ParThread to terminate next time it's unparked.
     *
     */
    public void terminate() {
        running = false;
        park.sync();
    }

    /**
     * Releases the ParThread to do some more work.
     */
    public void release() {
        park.sync();
    }

    /**
     * The main body of this process. above.
     */
    @Override
    public void run() {
        try {
            Parallel.addToAllParThreads(this);
            while (running) {
                try {
                    process.run();
                } catch (Throwable e) {
                    Parallel.uncaughtException("jcsp.lang.Parallel", e);
                }
                barrier.resign();
                park.sync();
            }
        } catch (Throwable t) {
            Parallel.uncaughtException("jcsp.lang.Parallel", t);
        } finally {
            Parallel.removeFromAllParThreads(this);
        }
    }
}
