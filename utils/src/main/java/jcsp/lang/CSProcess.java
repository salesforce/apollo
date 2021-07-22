
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
 * This is the JCSP interface for a <I>process</I> - an active component that
 * encapsulates the data structures on which it operates.
 * <H2>Description</H2> A
 * <A HREF="http://www.comlab.ox.ac.uk/archive/csp.html">CSP</A> <I>process</I>
 * is a component that encapsulates data structures and algorithms for
 * manipulating that data. Both its data and algorithms are private. The outside
 * world can neither see that data nor execute those algorithms. Each process is
 * alive, executing its own algorithms on its own data. Processes interact
 * solely via CSP synchronising primitives, such as <I>channels</I>,
 * <I>events</I> or other well-defined modes of access to shared passive
 * objects.
 * <P>
 * In this
 * <A HREF="http://www.hensa.ac.uk/parallel/languages/java/jcsp/">JCSP</A>
 * binding of the CSP model into Java, a process is an instance of a class
 * implementing this <TT>CSProcess</TT> interface. Its actions are defined by
 * the <TT>run</TT> method.
 * <P>
 * Running processes interact solely via CSP channels, events or carefully
 * synchronised access to shared objects -- not by calling each other's methods.
 * These passive objects form the <I>CSP interface</I> to a process. They are
 * not present in the <I>Java interface</I> and must be configured into each
 * process via public constructors and/or mutators when it is not running (i.e.
 * before and in between runs). It is safe to extract information from the
 * process via accessor methods, but only after (or in between) runs.
 * <P>
 * For other general information, see the JCSP
 * <A HREF="../../overview-summary.html">overview</A>.
 * <H2>Process Oriented Design</H2> A <I>process-oriented</I> design consists of
 * layered networks of processes. A <I>network</I> is simply a parallel
 * composition of processes connected through a set of passive synchronisation
 * objects (<I>wires</I>) and is itself a process. Each process fulfills a
 * contract with its environment that specifies not only what functions it
 * performs, but how it is prepared to synchronise with that environment to
 * obtain information and deliver results. Note that a process does not interact
 * directly with other processes, only with the wires to which it is connected.
 * This is a very familiar form of component interface -- certainly to hardware
 * engineers -- and is one that allows considerable flexibility and reuse.
 * <H3>Implementation Pattern</H3> The structure of a JCSP process should follow
 * the outline:
 * 
 * <PRE>
 * import jcsp.lang.*;
 * ...  other imports
 * <I> </I>
 * class ProcessExample implements CSProcess {
 * <I> </I>
 *   ...  private/protected shared synchronisation objects (channels etc.)
 *   ...  private/protected state information
 * <I> </I>
 *   ...  public constructors
 *   ...  public configuration/inspection methods (when not running)
 * <I> </I>
 *   ...  private/protected support methods (part of a run)
 *   ...  public run method (the process starts here)
 * <I> </I>
 * }
 * </PRE>
 * 
 * The pattern of use for these methods is simple and well-disciplined. The
 * public constructors and configuration methods must install the shared
 * synchronisation objects into their private/protected fields. They may also
 * initialise other private/protected state information. The public
 * configuration/inspection methods (simple <TT>set</TT>s and <TT>get</TT>s) may
 * be invoked only when this process is not running and are the responsibility
 * of a <I>single</I> process only -- usually the process that constructed it.
 * That process is also responsible for invoking the public <TT>run</TT> method
 * that kicks this one into life (usually, via a {@link Parallel} or
 * {@link ProcessManager} intermediary). The private support methods are
 * triggered only by the <TT>run</TT> method and express the live behaviour of
 * this process.
 * <P>
 * A process instance may have several lives but these must, of course, be
 * consecutive. Different instances of the same process class may, also of
 * course, be alive concurrently. Reconfiguration of a process via its
 * configuration/inspection methods may only take place between lives. Dynamic
 * reconfiguration of a live process may take place -- but only with the active
 * cooperation of the process (for example, through channel communication).
 * <H3>Motivation</H3> Like any object, a process should encapsulate a coherent
 * set of data and algorithms for managing that data. The discipline outlined in
 * the above implementation pattern has the following broad aims:
 * <UL>
 * <LI>maintain the encapsulation of information (which is something that is
 * easilly lost in free-wheeling OO-design as object references are passed
 * around);
 * <LI>enable a <I>precise</I> (CSP) specification of the way a process
 * interacts with its environment (represented in the above pattern by the
 * shared synchronisation objects);
 * <LI>express the behaviour of the process from its own point of view (which is
 * something not available to general OO-design whenever a method of one object
 * is invoked from the method of another);
 * <LI>eliminate the occurrence of race hazards upon the parallel composition of
 * processes;
 * <LI>enable formal certification of the absence of deadlock, livelock and
 * process starvation in a parallel system (through design-time analysis or the
 * checked adherence to higher-level CSP design rules for which proven safety
 * guarantees are known).
 * </UL>
 * These properties mean that processes make excellent building blocks for
 * complex systems -- their semantics compose cleanly and predictably as they
 * are plugged together.
 * <P>
 * This is not to say that the semantics of processes connected in parallel are
 * simply the sum of the semantics of each individual process -- just that those
 * semantics should contain no surprises. Value can added just through the
 * nature of the parallel composition itself. For example, connect simple
 * <I>stateless</I> processes into a channel feedback-loop, give them some
 * external wires and we get a component that contains <I>state</I>.
 * <H3>Discussion</H3> The public methods of an arbitrary <I>object</I> are at
 * the mercy of any other object that has a reference to it. Further,
 * invocations of those methods may be part of <I>any</I> thread of control.
 * Object designers, implementors and users need to have this under
 * well-documented control. This control is not easy to achieve without any
 * rules.
 * <P>
 * The process design pattern defines some powerful simplifications that bring
 * us that control. Backed up with the theory of CSP, it also addresses and
 * offers solutions to some fundamental issues in concurrency and
 * multi-threading.
 * <P>
 * In unconstrained OO, <I>threads</I> and <I>objects</I> are orthogonal notions
 * -- sometimes dangerously competitive. Threads have no internal structure and
 * can cross object boundaries in spaghetti-like trails that relate objects
 * together in a way that has little correspondence to the original OO design. A
 * <I>process</I> combines these ideas into a single concept, gives it structure
 * and maintains strong encapsulation by guarding the distribution and use of
 * references with some strict (and checkable) design rules. A process may be
 * wired up in parallel with other processes to form a network -- which is
 * itself a process. So, a process may have sub-processes that have
 * sub-sub-processes <I>ad infinitum</I>. This is the way the world works ...
 * <P>
 * The simplicity of the process design pattern is that each process can be
 * considered individually as an independant <I>serial</I> program, interacting
 * with external I/O devices (the synchronisation objects -- channels etc.). At
 * the other end of those devices may lie other processes, but that is of no
 * concern to the consideration of this process. All our past skills and
 * intuition about serial programming can safely be applied. The new skills
 * needed to design and use parallel process networks sit alongside those
 * earlier skills and no cross-interference takes place.
 * <P>
 * JCSP is an alternative to the built-in <I>monitor</I> model for Java threads.
 * JCSP primitives should not normally be mixed into designs with
 * <TT>synchronized</TT> method declarations, instances of the
 * <TT>java.lang.Runnable</TT> interface or <TT>java.lang.Thread</TT> class, or
 * invocations of the <TT>wait</TT>/<TT>notify</TT>/<TT>notifyAll</TT> methods
 * from <TT>java.lang.Object</TT>.
 * <P>
 * However, JCSP is compatible with the built-in model and, with care, can be
 * mixed safely and profitably. In particular, process communication via
 * <TT>wait</TT>-free <TT>synchronized</TT> method invocations on a shared
 * passive object directly implements a common CSP <I>server</I> idiom (see
 * {@link jcsp.awt.DisplayList <TT>DisplayList</TT>} for an example). Further,
 * existing libraries that interact with user software via <I>listener</I>
 * registration and callback (such as the standard <I>AWT</I> and <I>Swing</I>)
 * can be easily tailored to operate as processes with channel-based interfaces
 * (for example, see {@link jcsp.awt <I>jcsp.awt</I>}).
 * <P>
 * Finally, we note that the JCSP library reflects the
 * <A HREF="http://occam-pi.org/"><B>occam-pi</B></A> realisation of CSP and
 * pi-calculus., An <B>occam-pi</B> <TT>PROC</TT> declaration maps simply into a
 * class implementing {@link CSProcess <TT>CSProcess</TT>}, whose constructor
 * parameters mirror the <TT>PROC</TT> parameters and whose <TT>run</TT> method
 * mirrors the <TT>PROC</TT> body.
 * <P>
 * <H2>Examples</H2> Numerous examples are scattered throughout the
 * documentation of this library. Good starting ponts are in the
 * {@link Parallel} and {@link Alternative} classes. Here is another very simple
 * one:
 * 
 * <PRE>
 * import jcsp.lang.*;
 * <I></I>
 * // LaunchControl is a process to control the launch of a space rocket.
 * <I></I>
 * // It is configured with a countdown time in seconds -- this must be above
 * // a minimum threshold, MIN_COUNTDOWN, else the launch is immediately aborted.
 * <I></I>
 * // There are two control lines, abort and hold, that respectively abort
 * // or hold the launch if signalled.  The hold is released by a second
 * // signal on the same line.
 * <I></I>
 * // During countdown, the count is reported by outputting on the countdown
 * // channel.
 * <I></I>
 * // If not aborted, LaunchControl fires the rocket (by outputting on its fire
 * // channel) when the countdown reaches zero.  An ABORTED launch is also
 * // reported on this fire channel.
 * <I></I>
 * // After a successful or aborted launch, LaunchControl terminates.
 * // The status attribute records whether the launch was FIRED or ABORTED
 * // and may then be inspected.
 * <I></I>
 * public class LaunchControl implements CSProcess {
 * <I></I>
 *   public static final int MIN_COUNTDOWN = 10;
 * <I></I>
 *   public static final int FIRED = 0;
 *   public static final int HOLDING = 1;
 *   public static final int COUNTING = 2;
 *   public static final int ABORTED = 3;
 *   public static final int UNDEDFINED = 4;
 * <I></I>
 *   private final int start;
 *   private final AltingChannelInputInt abort;
 *   private final AltingChannelInputInt hold;
 *   private final ChannelOutputInt countdown;
 *   private final ChannelOutputInt fire;
 * <I></I>
 *   private int status = UNDEDFINED;
 * <I></I>
 *   public LaunchControl (final int start,
 *                         final AltingChannelInputInt abort,
 *                         final AltingChannelInputInt hold,
 *                         final ChannelOutputInt countdown,
 *                         final ChannelOutputInt fire) {
 *     this.start = start;
 *     this.abort = abort;
 *     this.hold = hold;
 *     this.countdown = countdown;
 *     this.fire = fire;
 *   }
 * <I></I>
 *   public int getStatus () {                   // inspection method
 *     return status;                            // (can only be used in between runs)
 *   }
 * <I></I>
 *   public void run () {
 * <I></I>
 *     final CSTimer tim = new CSTimer ();        // JCSP timers have
 *     final long oneSecond = 1000;               // millisecond granularity
 * <I></I>
 *     long timeout = tim.read () + oneSecond;    // compute first timeout
 * <I></I>
 *     final Alternative alt =
 *       new Alternative (new Guard[] {abort, hold, tim});
 *     final int ABORT = 0;
 *     final int HOLD = 1;
 *     final int TICK = 2;
 * <I></I>
 *     int count = start;
 *     boolean counting = (start >= MIN_COUNTDOWN); // abort if bad start
 * <I></I>
 *     while ((count > 0) && counting) {
 *       countdown.write (count);                   // public address system
 *       tim.setAlarm (timeout);                    // set next timeout
 *       switch (alt.priSelect ()) {
 *         case ABORT:                              // abort signalled
 *           abort.read ();                         // clear the signal
 *           counting = false;
 *         break;
 *         case HOLD:                               // hold signalled
 *           long timeLeft = timeout - tim.read (); // time till next tick
 *           hold.read ();                          // clear the signal
 *           fire.write (HOLDING);                  // signal rocket
 *           hold.read ();                          // wait for the release
 *           timeout = tim.read () + timeLeft;      // recompute next timeout
 *           fire.write (COUNTING);                 // signal rocket
 *         break;
 *         case TICK:                               // timeout expired
 *           count--;
 *           timeout += oneSecond;                  // compute next timeout
 *         break;
 *       }
 *     }
 * <I></I>
 *     status = (counting) ? FIRED : ABORTED;       // set status attribute
 *     fire.write (status);                         // signal rocket (go/nogo)
 *     if (counting) countdown.write (0);           // complete countdown
 *   }
 * }
 * </PRE>
 *
 * @see Parallel
 * @see jcsp.lang.PriParallel
 * @see Alternative
 * @see jcsp.lang.ChannelInput
 * @see jcsp.lang.ChannelOutput
 * @see jcsp.lang.One2OneChannel
 * @see jcsp.lang.Any2OneChannel
 * @see jcsp.lang.One2AnyChannel
 * @see jcsp.lang.Any2AnyChannel
 * @see jcsp.lang.ChannelInputInt
 * @see jcsp.lang.ChannelOutputInt
 * @see jcsp.lang.One2OneChannelInt
 * @see jcsp.lang.Any2OneChannelInt
 * @see jcsp.lang.One2AnyChannelInt
 * @see jcsp.lang.Any2AnyChannelInt
 * @see ProcessManager
 * @author P.D. Austin
 * @author P.H. Welch
 */

public interface CSProcess {
    /**
     * This defines the actions of the process.
     */
    public void run();
}
