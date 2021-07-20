
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
 * This enables <I>bucket</I> synchronisation between a set of processes.
 * <P>
 * <A HREF="#constructor_summary">Shortcut to the Constructor and Method
 * Summaries.</A>
 * 
 * <H2>Description</H2> A <I>bucket</I> is a non-deterministic cousin of a
 * {@link Barrier}. A bucket is somewhere to {@link #fallInto <TT>fallInto</TT>}
 * when a process needs somewhere to park itself. There is no limit on the
 * number of processes that can <TT>fallInto</TT> a bucket - and all are blocked
 * when they do. Release happens when a process (and it will have to be
 * <I>another</I> process) chooses to {@link #flush <TT>flush</TT>} that bucket.
 * When that happens, <I>all</I> processes in the bucket (which may be
 * <I>none</I>) are rescheduled for execution.
 * <P>
 * <TT>Bucket</TT>s are a <I>non-deterministic</I> primitive, since the decision
 * to flush is a free (<I>internal</I>) choice of the process concerned and the
 * scheduling of that flush impacts on the semantics. Usually, only <I>one</I>
 * process is given responsibility for <I>flushing</I> a bucket (or set of
 * buckets). Flushing a bucket does not block the flusher.
 * <P>
 * <I>Note:</I> this notion of bucket corresponds to the <A HREF=
 * "http://www.hensa.ac.uk/parallel/occam/projects/occam-for-all/hlps/">BUCKET</A>
 * synchronisation primitive added to the <A HREF=
 * "http://www.hensa.ac.uk/parallel/occam/projects/occam-for-all/kroc/">KRoC</A>
 * <B>occam</B> language system.
 * 
 * <H2>Implementation Note</H2> The {@link #fallInto <TT>fallInto</TT>} and
 * {@link #flush <TT>flush</TT>} methods of <TT>Bucket</TT> are just a
 * re-badging of the {@link Object#wait() <TT>wait</TT>} and
 * {@link Object#notifyAll() <TT>notifyAll</TT>} methods of {@link Object} - but
 * <I>without</I> the need to gain a monitor lock and <I>without</I> the need to
 * look out for the <TT>wait</TT> being interrupted.
 * <P>
 * Currently, though, this is how they are implemented. Beware that a
 * <TT>notifyAll</TT> carries an <TT>O(n)</TT> overhead (where n is the number
 * of processes being notified), since each notified process must regain the
 * monitor lock before it can exit the <TT>synchronized</TT> region. Future JCSP
 * implementations of <TT>Bucket</TT> will look to follow <B>occam</B> kernels
 * and reduce the overheads of both <TT>fallInto</TT> and <TT>flush</TT> to
 * <TT>O(1)</TT>.
 * 
 * <H2>A Simple Example</H2> This consists of 10 workers, one bucket and one
 * flusher:
 * <p>
 * <IMG SRC="doc-files\Bucket1.gif">
 * </p>
 * Here is the code for this network:
 * 
 * <PRE>
 * import jcsp.lang.*;
 * <I></I>
 * public class BucketExample1 {
 * <I></I>
 *   public static void main (String[] args) {
 * <I></I>
 *     final int nWorkers = 10;
 * <I></I>
 *     final int second = 1000;
 *     // JCSP timer units are milliseconds
 *     final int interval = 5*second;
 *     final int maxWork = 10*second;
 * <I></I>
 *     final long seed = new CSTimer ().read ();
 *     // for the random number generators
 * <I></I>
 *     final Bucket bucket = new Bucket ();
 * <I></I>
 *     final Flusher flusher = new Flusher (interval, bucket);
 * <I></I>
 *     final Worker[] workers = new Worker[nWorkers];
 *     for (int i = 0; i < workers.length; i++) {
 *       workers[i] = new Worker (i, i + seed, maxWork, bucket);
 *     }
 * <I></I>
 *     System.out.println ("*** Flusher: interval = " + interval
 *                         + " milliseconds");
 * <I></I>
 *     new Parallel (
 *       new CSProcess[] {
 *         flusher,
 *         new Parallel (workers)
 *       }
 *     ).run ();
 * <I></I>
 *   }
 * <I></I>
 * }
 * </PRE>
 * 
 * A <TT>Worker</TT> cycle consists of one shift (which takes a random amount of
 * time) and, then, falling into the bucket:
 * 
 * <PRE>
 * import jcsp.lang.*;
 * import java.util.*;
 * <I></I>
 * public class Worker implements CSProcess {
 * <I></I>
 *   private final int id;
 *   private final long seed;
 *   private final int maxWork;
 *   private final Bucket bucket;
 * <I></I>
 *   public Worker (int id, long seed, int maxWork, Bucket bucket) {
 *     this.id = id;
 *     this.seed = seed;
 *     this.maxWork = maxWork;
 *     this.bucket = bucket;
 *   }
 * <I></I>
 *   public void run () {
 * <I></I>
 *     final Random random = new Random (seed);
 *     // each process gets a different seed
 * <I></I>
 *     final CSTimer tim = new CSTimer ();
 *     final int second = 1000;
 *     // JCSP timer units are milliseconds
 * <I></I>
 *     final String working = "\t... Worker " + id
 *                            + " working ...";
 *     final String falling = "\t\t\t     ... Worker " + id
 *                            + " falling ...";
 *     final String flushed = "\t\t\t\t\t\t  ... Worker "
 *                            + id + " flushed ...";
 * <I></I>
 *     while (true) {
 *       System.out.println (working);
 *       tim.sleep (random.nextInt (maxWork));
 *       //These lines represent one unit of work
 * <i></i>
 *       System.out.println (falling);
 *       bucket.fallInto ();
 *       System.out.println (flushed);
 *     }
 *   }
 * <I></I>
 * }
 * </PRE>
 * 
 * The <TT>Flusher</TT> just flushes the bucket at preset time intervals:
 * 
 * <PRE>
 * import jcsp.lang.*;
 * import java.util.*;
 * <I></I>
 * public class Flusher implements CSProcess {
 * <I></I>
 *   private final int interval;
 *   private final Bucket bucket;
 * <I></I>
 *   public Flusher (int interval, Bucket bucket) {
 *     this.interval = interval;
 *     this.bucket = bucket;
 *   }
 * <I></I>
 *   public void run () {
 * <I></I>
 *     final CSTimer tim = new CSTimer ();
 *     long timeout = tim.read () + interval;
 * <I></I>
 *     while (true) {
 *       tim.after (timeout);
 *       System.out.println ("*** Flusher: about to flush ...");
 *       final int n = bucket.flush ();
 *       System.out.println ("*** Flusher: number flushed = " + n);
 *       timeout += interval;
 *     }
 *   }
 * <I></I>
 * }
 * </PRE>
 * 
 * <A NAME="Dingbats">
 * <H2>The Flying Dingbats</H2> This consists of <I>many</I> buckets, a
 * <I>single</I> bucket keeper (responsible for flushing the buckets) and flock
 * of <TT>Dingbat</TT>s (who regularly fall into various buckets). Here is the
 * system diagram:
 * <p>
 * <IMG SRC="doc-files\Bucket2.gif">
 * </p>
 * And here is the network code:
 * 
 * <PRE>
 * import jcsp.lang.*;
 * <I></I>
 * public class BucketExample2 {
 * <I></I>
 *   public static void main (String[] args) {
 * <I></I>
 *     final int minDingbat = 2;
 *     final int maxDingbat = 10;
 *     final int nDingbats = (maxDingbat - minDingbat) + 1;
 * <I></I>
 *     final int nBuckets = 2*maxDingbat;
 * <I></I>
 *     final Bucket[] bucket = Bucket.create (nBuckets);
 * <I></I>
 *     final int second = 1000;
 *     // JCSP timer units are milliseconds
 *     final int tick = second;
 *     final BucketKeeper bucketKeeper = new BucketKeeper (tick, bucket);
 * <I></I>
 *     final Dingbat[] dingbats = new Dingbat[nDingbats];
 *     for (int i = 0; i < dingbats.length; i++) {
 *       dingbats[i] = new Dingbat (i + minDingbat, bucket);
 *     }
 * <I></I>
 *     new Parallel (
 *       new CSProcess[] {
 *         bucketKeeper,
 *         new Parallel (dingbats)
 *       }
 *     ).run ();
 * <I></I>
 *   }
 * <I></I>
 * }
 * </PRE>
 * 
 * The <TT>BucketKeeper</TT> keeps time, flushing buckets in sequence at a
 * steady rate. When the last one has been flushed, it starts again with the
 * first:
 * 
 * <PRE>
 * import jcsp.lang.*;
 * <I></I>
 * class BucketKeeper implements CSProcess {
 * <I></I>
 *   private final long interval;
 *   private final Bucket[] bucket;
 * <I></I>
 *   public BucketKeeper (long interval, Bucket[] bucket) {
 *     this.interval = interval;
 *     this.bucket = bucket;
 *   }
 * <I></I>
 *   public void run () {
 * <I></I>
 *     String[] spacer = new String[bucket.length];
 *     spacer[0] = "";
 *     for (int i = 1; i < spacer.length; i++) {
 *       spacer[i] = spacer[i - 1] + "  ";
 *     }
 * <I></I>
 *     final CSTimer tim = new CSTimer ();
 *     long timeout = tim.read ();
 *     int index = 0;
 * <I></I>
 *     while (true) {
 *       final int n = bucket[index].flush ();
 *       if (n == 0) {
 *         System.out.println (spacer[index] + "*** bucket " +
 *                             index + " was empty ...");
 *       }
 *       index = (index + 1) % bucket.length;
 *       timeout += interval;
 *       tim.after (timeout);
 *     }
 *   }
 * <I></I>
 * }
 * </PRE>
 * 
 * So the buckets represent time values. A process falling into one of them will
 * sleep until the prescribed time when the <TT>BucketKeeper</TT> next flushes
 * it.
 * <P>
 * <TT>Dingbat</TT>s live the following cycle. First, they do some work (rather
 * brief in the following code). Then, they work out which bucket to fall into
 * and fall into it - that is all. In this case, <TT>Dingbat</TT>s just fly on
 * <TT>id</TT> buckets from whence they were just flushed (where <TT>id</TT> is
 * the <TT>Dingbat</TT> number indicated in the <A HREF="#Dingbats">above
 * network diagram</A>):
 * 
 * <PRE>
 * import jcsp.lang.*;
 * <I></I>
 * public class Dingbat implements CSProcess {
 * <I></I>
 *   private final int id;
 *   private final Bucket[] bucket;
 * <I></I>
 *   public Dingbat (int id, Bucket[] bucket) {
 *     this.id = id;
 *     this.bucket = bucket;
 *   }
 * <I></I>
 *   public void run () {
 * <I></I>
 *     int logicalTime = 0;
 * <I></I>
 *     String[] spacer = new String[bucket.length];
 *     spacer[0] = "";
 *     for (int i = 1; i < spacer.length; i++) {
 *       spacer[i] = spacer[i - 1] + "  ";
 *     }
 * <I></I>
 *     String message = "Hello world from " + id + " ==> time = ";
 * <I></I>
 *     while (true) {
 *       logicalTime += id;
 *       final int slot = logicalTime % bucket.length;
 *       // assume: id <= bucket.length
 *       bucket[slot].fallInto ();
 *       System.out.println (spacer[slot] + message + logicalTime);
 *       // one unit of work
 *     }
 *   }
 * <I></I>
 * }
 * </PRE>
 *
 * <H3>Danger - Race Hazard</H3> This example contains a race hazard whose
 * elimination is left as an exercise for the reader. The problem is to ensure
 * that all flushed <TT>Dingbat</TT>s have settled in their next chosen buckets
 * <I>before</I> the <TT>BucketKeeper</TT> next flushes it. If a
 * <TT>Dingbat</TT> is a bit slow, it may fall into its chosen bucket too late -
 * the <TT>BucketKeeper</TT> has already flushed it and the creature will have
 * to remain there until the next cycle.
 * <P>
 * With the rate of flushing used in the above system, this is unlikely to
 * happen. But it is just possible - if something suspended execution of the
 * system for a few seconds immediately following a flush, then the
 * <TT>BucketKeeper</TT> could be rescheduled before the flying
 * <TT>Dingbat</TT>s.
 *
 * <H3>Acknowledgement</H3> This example is from a discrete event modelling
 * approach due to Jon Kerridge (Napier University, Scotland). The
 * <TT>Dingbat</TT>s could easilly model their own timeouts for themselves.
 * However, setting a timeout is an <TT>O(n)</TT> operation (where <TT>n</TT> is
 * the number of processes setting them). Here, there is only one process
 * setting timeouts (the <TT>BucketKeeper</TT>) and the bucket operations
 * <TT>fallInto</TT> and <TT>flush</TT> have <TT>O(1)</TT> costs (at least, that
 * is the case for <B>occam</B>).
 * <P>
 * Of course, removal of the above race hazard means that the <I>timeout</I> by
 * the <TT>BucketKeeper</TT> can also be eliminated. The buckets can be flushed
 * just as soon as it knows that the previously flushed <TT>Dingbat</TT>s have
 * settled. In this way, the event model can proceed at full speed, maintaining
 * correct simulated time - each bucket representing one time unit - without
 * needing any timeouts in the simulation itself.
 * </P>
 *
 * @see Barrier
 * 
 * @author P.H. Welch
 */

public class Bucket implements Serializable {
    private static final long serialVersionUID = 1L;
    /**
     * The number of processes currently enrolled on this bucket.
     */
    private int               nHolding         = 0;

    /**
     * The monitor lock used for synchronization
     */
    private final Object bucketLock = new Object();

    /**
     * Barrier uses an even/odd flag because the barrier cannot sync without every
     * process Bucket can happily keep working while old processes are waiting
     * around, so a flag is not enough Instead, a count must be used. Theoretically
     * this is unsafe, but the likelihood of the bucket completing 4 *billion*
     * cycles before the process wakes up is somewhat slim.
     */
    private int bucketCycle = 0;

    /**
     * Fall into the bucket. The process doing this will be blocked until the next
     * {@link #flush}.
     */
    public void fallInto() {
        synchronized (bucketLock) {
            nHolding++;
            // System.out.println ("Bucket.fallInto : " + nHolding);
            try {
                int spuriousCycle = bucketCycle;
                bucketLock.wait();
                while (spuriousCycle == bucketCycle) {
                    if (Spurious.logging) {
                        SpuriousLog.record(SpuriousLog.BucketFallInto);
                    }
                    bucketLock.wait();
                }
            } catch (InterruptedException e) {
                throw new ProcessInterruptedException("*** Thrown from Bucket.fallInto ()\n" + e.toString());
            }
        }
    }

    /**
     * Flush the bucket. All held processes will be released. It returns the number
     * that were released.
     * <P>
     * 
     * @return the number of processes flushed.
     */
    public int flush() {
        synchronized (bucketLock) {
            // System.out.println ("Bucket.flush : " + nHolding);
            final int tmp = nHolding;
            nHolding = 0;
            bucketCycle += 1;
            bucketLock.notifyAll();
            return tmp;
        }
    }

    /**
     * This returns the number of processes currently held in the bucket. Note that
     * this number is <I>volatile</I> - for information only! By the time the
     * invoker of this method receives it, it might have changed (because of further
     * processes falling into the bucket or someone flushing it).
     * <P>
     * 
     * @return the number of processes currently held in the bucket.
     */
    public int holding() {
        synchronized (bucketLock) {
            return nHolding;
        }
    }

    /**
     * Creates an array of Buckets.
     * <P>
     *
     * @param n the number of Buckets to create in the array
     * @return the array of <TT>Bucket</TT>s
     */
    public static Bucket[] create(int n) {
        Bucket[] buckets = new Bucket[n];
        for (int i = 0; i < n; i++) {
            buckets[i] = new Bucket();
        }
        return buckets;
    }

}
