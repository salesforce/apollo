
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
 * This enables a process to wait passively for and choose between a number of
 * {@link Guard} events.
 * <P>
 * <A HREF="#constructor_summary">Shortcut to the Constructor and Method
 * Summaries.</A>
 * <H2>Description</H2> The <code>Alternative</code> class enables a
 * <code>CSProcess</code> to wait passively for and choose between a number of
 * {@link Guard} events. This is known as <code>ALT</code><I>ing</I>.
 * <P>
 * <I>Note: for those familiar with the <I><B>occam</B></I> multiprocessing
 * language, this gives the semantics of the </I><code>ALT</code><I> and
 * </I><code>PRI</code> <code>ALT</code><I> constructs, extended with a built-in
 * implementation of the classical </I><code>FAIR</code>
 * <code>ALT</code><I>.</I>
 * <P>
 * The <code>Alternative</code> constructor takes an array of guards. Processes
 * that need to <I>Alt</I> over more than one set of guards will need a separate
 * <code>Alternative</code> instance for each set.
 * <P>
 * Eight types of <code>Guard</code> are provided in <code>jcsp.lang</code>:
 * <UL>
 * <LI>{@link AltingChannelInput}: <I>object channel input</I> -- ready if
 * unread data is pending in the channel.
 * <LI>{@link AltingChannelInputInt}: <I>integer channel input</I> -- ready if
 * unread data is pending in the channel.
 * <LI>{@link AltingChannelOutput}: <I>object channel output</I> -- ready if a
 * reading process can take the offered data ({@link One2OneChannelSymmetric
 * <i>symmetric</i>} channels only).
 * <LI>{@link AltingChannelOutputInt}: <I>integer channel output</I> -- ready if
 * a reading process can take the offered data
 * ({@link One2OneChannelSymmetricInt <i>symmetric</i>} channels only).
 * <LI>{@link AltingChannelAccept}: <I>CALL accept</I> -- ready if an unaccepted
 * call is pending.
 * <LI>{@link AltingBarrier}: <I>barrier synchronisation</I> -- ready if all
 * enrolled processes are offering to synchronise.
 * <LI>{@link CSTimer}: <I>timeout</I> -- ready if the timeout has expired
 * (timeout values are absolute time values, not delays)
 * <LI>{@link Skip}: <I>skip</I> -- always ready.
 * </UL>
 * <P>
 * By invoking one of the following methods, a process may passively wait for
 * one or more of the guards associated with an <code>Alternative</code> object
 * to become ready. The methods differ in the way they choose which guard to
 * select in the case when two or more guards are ready:
 * <UL>
 * <LI>{@link #select() <code>select</code>} waits for one or more of the guards
 * to become ready. If more than one become ready, it makes an <I>arbitrary</I>
 * choice between them (and corresponds to the <I><B>occam</B></I>
 * <code>ALT</code>).
 * <LI>{@link #priSelect() <code>priSelect</code>} also waits for one or more of
 * the guards to become ready. However, if more than one becomes ready, it
 * chooses the <I>first</I> one listed (and corresponds to the
 * <I><B>occam</B></I> <code>PRI</code> <code>ALT</code>). Note: the use of
 * <code>priSelect</code> between channel inputs and a skip guard (at lowest
 * priority) gives us a <I>polling</I> operation on the <I>readiness</I> of
 * those channels.</I>
 * <LI>{@link #fairSelect() <code>fairSelect</code>} also waits for one or more
 * of the guards to become ready. If more than one become ready, it prioritises
 * its choice so that the guard it chose <I>the last time it was invoked</I> has
 * lowest priority this time. This corresponds to a common <I><B>occam</B></I>
 * idiom used for real-time applications. If <code>fairSelect</code> is used in
 * a loop, a ready guard has the guarantee that no other guard will be serviced
 * <I>twice</I> before it will be serviced. This enables an upper bound on
 * service times to be calculated and ensures that no ready guard can be
 * indefinitely starved.
 * </UL>
 * <P>
 * Finally, each guard may be <A HREF="Alternative.html#Wot-no-Chickens">
 * <I>pre-conditioned</I></A> with a run-time test to decide if it should be
 * considered in the current choice. This allows considerable flexibilty -- for
 * example, we can decide whether timeouts shoud be set, channels refused or
 * polling enabled depending on the run-time state of the <I>Alting</I> process.
 * </P>
 * <H2>Examples</H2>
 * <H3>A Fair Multiplexor</H3> This example demonstrates a process that
 * <I>fairly</I> multiplexes traffic from its array of input channels to its
 * single output channel. No input channel will be starved, regardless of the
 * eagerness of its competitors.
 * 
 * <PRE>
 * import jcsp.lang.*;
 * <I></I>
 * public class FairPlex implements CSProcess {
 * <I></I>
 *   private final AltingChannelInput[] in;
 *   private final ChannelOutput out;
 * <I></I>
 *   public FairPlex (final AltingChannelInput[] in, final ChannelOutput out) {
 *     this.in = in;
 *     this.out = out;
 *   }
 * <I></I>
 *   public void run () {
 * <I></I>
 *     final Alternative alt = new Alternative (in);
 * <I></I>
 *     while (true) {
 *       final int index = alt.fairSelect ();
 *       out.write (in[index].read ());
 *     }
 * <I></I>
 *   }
 * <I></I>
 * }
 * </PRE>
 * 
 * Note that if <code>priSelect</code> were used above, higher-indexed channels
 * would be starved if lower-indexed channels were continually demanding
 * service. If <code>select</code> were used, no starvation analysis is
 * possible. The <code>select</code> mechanism should only be used when
 * starvation is not an issue.
 * 
 * <H3><A NAME="FairMuxTime">A Fair Multiplexor with a Timeout and
 * Poisoning</H3> This example demonstrates a process that <I>fairly</I>
 * multiplexes traffic from its input channels to its single output channel, but
 * which timeouts after a user-settable time. Whilst running, no input channel
 * will be starved, regardless of the eagerness of its competitors. The process
 * also illustrates the poisoning of channels, following the timeout.
 * 
 * <PRE>
 * import jcsp.lang.*;
 * 
 * public class FairPlexTime implements CSProcess {
 * 
 *     private final AltingChannelInput[] in;
 *     private final ChannelOutput out;
 *     private final long timeout;
 * 
 *     public FairPlexTime(final AltingChannelInput[] in, final ChannelOutput out, final long timeout) {
 *         this.in = in;
 *         this.out = out;
 *         this.timeout = timeout;
 *     }
 * 
 *     public void run() {
 * 
 *         final Guard[] guards = new Guard[in.length + 1];
 *         System.arraycopy(in, 0, guards, 0, in.length);
 * 
 *         final CSTimer tim = new CSTimer();
 *         final int timerIndex = in.length;
 *         guards[timerIndex] = tim;
 * 
 *         final Alternative alt = new Alternative(guards);
 * 
 *         boolean running = true;
 *         tim.setAlarm(tim.read() + timeout);
 *         while (running) {
 *             final int index = alt.fairSelect();
 *             if (index == timerIndex) {
 *                 running = false;
 *             } else {
 *                 out.write(in[index].read());
 *             }
 *         }
 *         System.out.println("\n\r\tFairPlexTime: timed out ... poisoning all channels ...");
 *         for (int i = 0; i &lt; in.length; i++) {
 *             in[i].poison(42); // assume: channel immunity &lt; 42
 *         }
 *         out.poison(42); // assume: channel immunity &lt; 42
 * 
 *     }
 * 
 * }
 * </PRE>
 * 
 * Note that if <code>priSelect</code> were used above, higher-indexed guards
 * would be starved if lower-indexed guards were continually demanding service
 * -- and the timeout would never be noticed. If <code>select</code> were used,
 * no starvation analysis is possible.
 * <P>
 * Sometimes we need to use <code>priSelect</code> to impose a <I>specific</I>
 * (as opposed to <I>fair</I>) choice that overcomes the external scheduling of
 * events. For example, if we were concerned that the timeout above should be
 * responded to <I>immediately</I> and unconcerned about the fair servicing of
 * its channels, we should put its <code>CSTimer</code> as the first element of
 * its <code>Guard</code> array and use <code>priSelect</code>.
 * <P>
 * To demonstrate <code>FairPlexTime</code>, consider:
 * 
 * <PRE>
 * import jcsp.lang.*;
 * import jcsp.plugNplay.*;
 * 
 * class FairPlexTimeTest {
 * 
 *     public static void main(String[] args) {
 * 
 *         final One2OneChannel[] a = Channel.one2OneArray(5, 0); // poisonable channels (zero immunity)
 *         final One2OneChannel b = Channel.one2One(0); // poisonable channels (zero immunity)
 *
 *         final long timeout = 5000; // 5 seconds
 * 
 *         new Parallel(new CSProcess[] { new Generate(a[0].out(), 0), new Generate(a[1].out(), 1),
 *                                        new Generate(a[2].out(), 2), new Generate(a[3].out(), 3),
 *                                        new Generate(a[4].out(), 4),
 *                                        new FairPlexTime(Channel.getInputArray(a), b.out(), timeout),
 *                                        new Printer(b.in(), "FairPlexTimeTest ==> ", "\n") }).run();
 * 
 *     }
 * 
 * }
 * </PRE>
 * 
 * where {@link jcsp.plugNplay.Generate} sends its given <tt>Integer</tt> down
 * its output channel as often as it can. This results in continuous demands on
 * <tt>FairPlexTime</tt> by all its clients and demonstrates its fair servicing
 * of those demands.
 * <P>
 * The {@link jcsp.plugNplay.Generate} and {@link jcsp.plugNplay.Printer} are
 * programmed to deal with being poisoned. Here is the <tt>run()</tt> method for
 * <tt>Generate</tt>:
 * 
 * <PRE>
 * public void run() {
 *     try {
 *         while (true) {
 *             out.write(N);
 *         }
 *     } catch (PoisonException p) {
 *         // the 'out' channel must have been posioned ... nothing left to do!
 *     }
 * }
 * </PRE>
 * 
 * In general, there will be things to do &ndash; especially if there is more
 * than one channel. For example, here is the <tt>catch</tt> block at the end of
 * the <tt>run()</tt> method for {@link jcsp.plugNplay.Delta} (which has a
 * single input channel-end, <tt>in</tt>, and an array of output channel-ends,
 * <tt>out</tt>):
 * 
 * <PRE>
 *   } catch (PoisonException p) {
 *     // don't know which channel was posioned ... so, poison them all!
 *     int strength = p.getStrength ();   // use same strength of poison
 *     in.poison (strength);
 *     for  (int i = 0; i &lt; out.length; i++) {
 *       out[i].poison (strength);
 *     }
 *   }
 * </PRE>
 *
 * <H3><A NAME="STFR">A Simple Traffic Flow Regulator</H3> The
 * <code>Regulate</code> process controls the rate of flow of traffic from its
 * input to output channels. It produces a constant rate of output flow,
 * regardless of the rate of its input. At the end of each timeslice defined by
 * the required output rate, it outputs the last object input during that
 * timeslice. If nothing has come in during a timeslice, the previous output
 * will be repeated (note: this will be a <code>null</code> if nothing has ever
 * arrived). If the input flow is greater than the required output flow, data
 * will be discarded.
 * <P>
 * The interval (in msecs) defining the output flow rate is given by a
 * constructor argument. This can be changed at any time by sending a new
 * interval (as a <code>Long</code>) down the <code>reset</code> channel.
 * <P>
 * <I> Note: this example shows how simple it is to program time-regulated
 * functionality like that performed by
 * </I><code>java.awt.Component.repaint</code><I>.</I>
 * 
 * <PRE>
 *
 * //////////////////////////////////////////////////////////////////////
 * //                                                                  //
 * //  JCSP ("CSP for Java") Libraries                                 //
 * //  Copyright (C) 1996-2018 Peter Welch, Paul Austin and Neil Brown //
 * //                2001-2004 Quickstone Technologies Limited         //
 * //                2005-2018 Kevin Chalmers                          //
 * //                                                                  //
 * //  You may use this work under the terms of either                 //
 * //  1. The Apache License, Version 2.0                              //
 * //  2. or (at your option), the GNU Lesser General Public License,  //
 * //       version 2.1 or greater.                                    //
 * //                                                                  //
 * //  Full licence texts are included in the LICENCE file with        //
 * //  this library.                                                   //
 * //                                                                  //
 * //  Author contacts: P.H.Welch@kent.ac.uk K.Chalmers@napier.ac.uk   //
 * //                                                                  //
 * //////////////////////////////////////////////////////////////////////
 * 
 * package jcsp.plugNplay;
 *
 * import jcsp.lang.*;
 * 
 * public class Regulate implements CSProcess {
 * 
 *     private final AltingChannelInput in, reset;
 *     private final ChannelOutput out;
 *     private final long initialInterval;
 * 
 *     public Regulate(final AltingChannelInput in, final AltingChannelInput reset, final ChannelOutput out,
 *                     final long initialInterval) {
 *         this.in = in;
 *         this.reset = reset;
 *         this.out = out;
 *         this.initialInterval = initialInterval;
 *     }
 * 
 *     public void run() {
 * 
 *         final CSTimer tim = new CSTimer();
 * 
 *         final Guard[] guards = { reset, tim, in }; // prioritised order
 *         final int RESET = 0; // index into guards
 *         final int TIM = 1; // index into guards
 *         final int IN = 2; // index into guards
 * 
 *         final Alternative alt = new Alternative(guards);
 * 
 *         Object x = null; // holding object
 * 
 *         long interval = initialInterval;
 * 
 *         long timeout = tim.read() + interval;
 *         tim.setAlarm(timeout);
 * 
 *         while (true) {
 *             switch (alt.priSelect()) {
 *             case RESET:
 *                 interval = ((Long) reset.read()).longValue();
 *                 timeout = tim.read(); // fall through
 *             case TIM:
 *                 out.write(x);
 *                 timeout += interval;
 *                 tim.setAlarm(timeout);
 *                 break;
 *             case IN:
 *                 x = in.read();
 *                 break;
 *             }
 *         }
 * 
 *     }
 * 
 * }
 * </PRE>
 * <P>
 * To demonstrate <code>Regulate</code>, consider:
 * 
 * <PRE>
 * class RegulateTest {
 * 
 *     public static void main(String[] args) {
 * 
 *         final One2OneChannel a = Channel.one2One();
 *         final One2OneChannel b = Channel.one2One();
 *         final One2OneChannel c = Channel.one2One();
 * 
 *         final One2OneChannel reset = Channel.one2one(new OverWriteOldestBuffer(1));
 * 
 *         new Parallel(new CSProcess[] { new Numbers(a.out()), // generate numbers
 *                                        new FixedDelay(250, a.in(), b.out()), // let them through every quarter second
 *                                        new Regulate(b.in(), reset.in(), c.out(), 1000), // initially sample every
 *                                                                                         // second
 *                                        new CSProcess() {
 *                                            public void run() {
 *                                                Long[] sample = { new Long(1000), new Long(250), new Long(100) };
 *                                                int[] count = { 10, 40, 100 };
 *                                                while (true) {
 *                                                    for (int cycle = 0; cycle < sample.length; cycle++) {
 *                                                        reset.write(sample[cycle]);
 *                                                        System.out.println("\nSampling every " + sample[cycle]
 *                                                        + " ms ...\n");
 *                                                        for (int i = 0; i < count[cycle]; i++) {
 *                                                            Integer n = (Integer) c.read();
 *                                                            System.out.println("\t==> " + n);
 *                                                        }
 *                                                    }
 *                                                }
 *                                            }
 *                                        } }).run();
 *     }
 * 
 * }
 * </PRE>
 * 
 * The reader may like to consider the danger of deadlock in the above system if
 * the <code>reset</code> channel were not an <I>overwriting</I> one.
 *
 * <A NAME="Polling">
 * <H3>Polling</H3> Sometimes, we want to handle incoming channel data if it's
 * there, but get on with something else if all is quiet. This can be done by
 * <code>PRI</code> <code>ALT</code><I>ing</I> the channels we wish to poll
 * against a <code>SKIP</code> guard:
 * 
 * <PRE>
 * import jcsp.lang.*;
 * 
 * public class Polling implements CSProcess {
 * 
 *   private final AltingChannelInput in0;
 *   private final AltingChannelInput in1;
 *   private final AltingChannelInput in2;
 *   private final ChannelOutput out;
 * 
 *   public Polling (final AltingChannelInput in0, final AltingChannelInput in1,
 *                   final AltingChannelInput in2, final ChannelOutput out) {
 *     this.in0 = in0;
 *     this.in1 = in1;
 *     this.in2 = in2;
 *     this.out = out;
 *   }
 * 
 *   public void run() {
 * 
 *     final Skip skip = new Skip ();
 *     final Guard[] guards = {in0, in1, in2, skip};
 *     final Alternative alt = new Alternative (guards);
 * 
 *     while (true) {
 *       switch (alt.priSelect ()) {
 *         case 0:
 *           ...  process data pending on channel in0 ...
 *         break;
 *         case 1:
 *           ...  process data pending on channel in1 ...
 *         break;
 *         case 2:
 *           ...  process data pending on channel in2 ...
 *         break;
 *         case 3:
 *           ...  nothing available for the above ...
 *           ...  so get on with something else for a while ...
 *           ...  then loop around and poll again ...
 *         break;
 *       }
 *     }
 * 
 *   }
 * 
 * }
 * </PRE>
 * 
 * The above technique lets us poll <I>any</I> {@link Guard} events, including
 * timeouts. If we just want to poll <I>channels</I> for input events, see the
 * {@link AltingChannelInput#pending pending} methods of the various
 * ``<TT>...2One...</TT>'' channels for a more direct and efficient way.
 * <P>
 * <I>Note: polling is an often overused technique. Make sure your design would
 * not be better suited with a blocking ALT and with the `something else' done
 * by a process running in parallel.</I>
 *
 * <A NAME="Wot-no-Chickens">
 * <H3>The <A HREF=
 * "http://wotug.org/parallel/groups/wotug/java/discussion/">`Wot-no-Chickens?'</A>
 * Canteen</H3> This examples demonstrates the use of <I>pre-conditions</I> on
 * the <code>ALT</code> guards. The <code>Canteen</code> process buffers a
 * supply of chickens. It can hold a maximum of 20 chickens. Chickens are
 * supplied on the <code>supply</code> line in batches of, at most, 4. Chickens
 * are requested by hungry philosophers who share the <code>request</code> line
 * to the <code>Canteen</code>. In response to such requests, one chicken is
 * delivered down the <code>deliver</code> line.
 * <P>
 * The <code>Canteen</code> refuses further supplies if it has no room for the
 * maximum (4) batch supply. The <code>Canteen</code> refuses requests from the
 * philosophers if it has no chickens.
 * 
 * <PRE>
 * import jcsp.lang.*;
 * 
 * public class Canteen implements CSProcess {
 * 
 *     private final AltingChannelInput supply; // from the cook
 *     private final AltingChannelInput request; // from a philosopher
 *     private final ChannelOutput deliver; // to a philosopher
 * 
 *     public Canteen(final AltingChannelInput supply, final AltingChannelInput request, final ChannelOutput deliver) {
 *         this.supply = supply;
 *         this.request = request;
 *         this.deliver = deliver;
 *     }
 * 
 *     public void run() {
 * 
 *         final Guard[] guard = { supply, request };
 *         final boolean[] preCondition = new boolean[guard.length];
 *         final int SUPPLY = 0;
 *         final int REQUEST = 1;
 * 
 *         final Alternative alt = new Alternative(guard);
 * 
 *         final int maxChickens = 20;
 *         final int maxSupply = 4;
 *         final int limitChickens = maxChickens - maxSupply;
 * 
 *         final Integer oneChicken = new Integer(1);
 *         // ready to go!
 * 
 *         int nChickens = 0;
 *         // invariant : 0 <= nChickens <= maxChickens
 * 
 *         while (true) {
 *             preCondition[SUPPLY] = (nChickens <= limitChickens);
 *             preCondition[REQUEST] = (nChickens > 0);
 *             switch (alt.priSelect(preCondition)) {
 *             case SUPPLY:
 *                 nChickens += ((Integer) supply.read()).intValue(); // <= maxSupply
 *                 break;
 *             case REQUEST:
 *                 Object dummy = request.read();
 *                 // we have to still input the signal
 *                 deliver.write(oneChicken);
 *                 // preCondition ==> (nChickens > 0)
 *                 nChickens--;
 *                 break;
 *             }
 *         }
 * 
 *     }
 * 
 * }
 * </PRE>
 * <P>
 * Contrast the above programming of the canteen as a CSP <I>process</I> rather
 * than a <I>monitor</I>. A monitor cannot refuse a callback when noone has the
 * lock, even though it may not be in a state to process it. In the above, a
 * <code>supply</code> <I>method</I> would have to cope with its being called
 * when there is no room to take the supply. A <code>request</code>
 * <I>method</I> would have to be dealt with even though there may be no
 * chickens to deliver. Monitors manage such problems by putting their callers
 * on hold (<code>wait</code>), but that means that their methods have to rely
 * on each other to get out of any resulting embarassment (using
 * <code>notify</code>). And that means that the logic of those methods has to
 * be tightly coupled, which makes reasoning about them hard. This gets worse
 * the more interdependent methods the monitor has.
 * <P>
 * On the other hand, the above <code>Canteen</code> <I>process</I> simply
 * refuses service on its <code>supply</code> and <code>request</code>
 * <I>channels</I> if it can't cope, leaving the supplying or requesting
 * processes waiting harmlessly on those channels. The service responses can
 * assume their run-time set <I>pre-conditions</I> and have independent -- and
 * trivial -- logic. When circumstances permit, the blocked processes are
 * serviced in the normal way.
 * </P>
 * <H2>Implementation Footnote</H2> This <code>Alternative</code> class and the
 * various channel classes (e.g. {@link One2OneChannel}) are mutually dependent
 * monitors -- they see instances of each other and invoke each others' strongly
 * interdependent methods. This logic is inspired by the published algorithms
 * and data structures burnt into the microcode of the <I>transputer</I> some 15
 * years ago (1984). Getting this logic <I>`right'</I> in the context of Java
 * monitors is something we have done <code>(n + 1)</code> times, only to find
 * it flawed <code>n</code> times with an unsuspected race-hazard months
 * (sometimes years) later. Hopefully, we have it <I>right</I> now ... but a
 * proof of correctness is really needed!
 * </P>
 * To this end, a formal (CSP) model of Java's monitor primitives (the
 * <code>synchronized</code> keyword and the <code>wait</code>,
 * <code>notify</code> and <code>notifyAll</code> methods of the
 * <code>Object</code> class) has been built. This has been used for the
 * <I>formal verification</I> of the JCSP implementation of channel
 * <code>read</code> and <code>write</code>, along with the correctness of
 * <I>2-way</I> channel input <code>Alternative</code>s. Details and references
 * are listed under
 * <A HREF="http://www.cs.kent.ac.uk/projects/ofa/jcsp/index.html#Model"><I>`A
 * CSP Model for Java Threads'</I> on the JCSP web-site</A>. [The proof uses the
 * <A HREF="http://www.formal.demon.co.uk/FDR2.html">FDR</A> model checker.
 * Model checkers do not easily allow verification of results containing free
 * variables - such as the correctness of the <I>n-way</I>
 * <code>Alternative</code>. An investigation of this using <I>formal
 * transformation</I> of one system of CSP equations into another, rather than
 * <I>model checking</I> is being considered.]
 * <P>
 * The <I>transputer</I> designers always said that getting its microcoded
 * scheduler logic right was one of their hardest tasks. Working directly with
 * the monitor concept means working at a similar level of difficulty for
 * application programs. One of the goals of JCSP is to protect users from ever
 * having to work at that level, providing instead a range of CSP primitives
 * whose ease of use scales well with application complexity -- and in whose
 * implementation those monitor complexities are correctly distilled and hidden.
 *
 * @see Guard
 * @see AltingChannelInput
 * @see AltingChannelInputInt
 * @see AltingChannelAccept
 * @see AltingBarrier
 * @see CSTimer
 * @see Skip
 * 
 * @author P.H. Welch and P.D. Austin
 */
//}}}

public class Alternative {
    /** The monitor synchronising the writers and alting reader */
    protected Object altMonitor = new Object();

    private static final int enabling = 0;
    private static final int waiting  = 1;
    private static final int ready    = 2;
    private static final int inactive = 3;

    /** The state of the ALTing process. */
    private int state = inactive;

    /** The array of guard events from which we are selecting. */
    private final Guard[] guard;

    /** The index of the guard with highest priority for the next select. */
    private int favourite = 0; // invariant: 0 <= favourite < guard.length

    /** The index of the selected guard. */
    private int selected; // after the enable/disable sequence :
                          // 0 <= selected < guard.length

    private final int NONE_SELECTED = -1;

    /** This indicates whether an AltingBarrier is one of the Guards. */
    private boolean barrierPresent;

    /** This flag is set by a successful AltingBarrier enable/disable. */
    private boolean barrierTrigger = false;

    /** The index of a selected AltingBarrier. */
    private int barrierSelected;

    /**
     * This is the index variable used during the enable/disable sequences. This has
     * been made global to simplify the call-back (setTimeout) from a CSTimer that
     * is being enabled. That call-back sets the timeout, msecs and timeIndex
     * variables below. The latter variable is needed only to work around the bug
     * that Java wait-with-timeouts sometimes return early.
     */
    private int enableIndex;

    /** This flag is set if one of the enabled guards was a CSTimer guard. */
    private boolean timeout = false;

    /** If one or more guards were CSTimers, this holds the earliest timeout. */
    private long msecs;

    /**
     * If one or more guards were CSTimers, this holds the index of the one with the
     * earliest timeout.
     */
    private int timeIndex;

    /**
     * Construct an <code>Alternative</code> object operating on the {@link Guard}
     * array of events. Supported guard events are channel inputs
     * ({@link AltingChannelInput} and {@link AltingChannelInputInt}), CALL channel
     * accepts ({@link AltingChannelAccept}), barriers ({@link AltingBarrier}),
     * timeouts ({@link CSTimer}) and skips ({@link Skip}).
     * <P>
     *
     * @param guard the event guards over which the select operations will be made.
     */
    public Alternative(final Guard[] guard) {
        this.guard = guard;
        for (int i = 0; i < guard.length; i++) {
            if (guard[i] instanceof MultiwaySynchronisation) {
                barrierPresent = true;
                return;
            }
        }
        barrierPresent = false;
    }

    /**
     * Returns the index of one of the ready guards. The method will block until one
     * of the guards becomes ready. If more than one is ready, an <I>arbitrary</I>
     * choice is made.
     */
    public final int select() {
        return fairSelect(); // a legal implementation of arbitrary choice!
    }

    /**
     * Returns the index of one of the ready guards. The method will block until one
     * of the guards becomes ready. If more than one is ready, the one with the
     * lowest index is selected.
     */
    public final int priSelect() {
        // if (barrierPresent) {
        // throw new AlternativeError (
        // "*** Cannot 'priSelect' with an AltingBarrier in the Guard array"
        // );
        // }
        state = enabling;
        favourite = 0;
        enableGuards();
        synchronized (altMonitor) {
            if (state == enabling) {
                state = waiting;
                try {
                    if (timeout) {
                        long delay = msecs - System.currentTimeMillis();
                        if (delay > Spurious.earlyTimeout) {
                            altMonitor.wait(delay);
                            /*
                             * while ((state == waiting) && ((delay = (msecs - System.currentTimeMillis ()))
                             * > Spurious.earlyTimeout)) { if (Spurious.logging) { SpuriousLog.record
                             * (SpuriousLog.AlternativeSelectWithTimeout); } altMonitor.wait (delay); } if
                             * ((state == waiting) && (delay > 0)) { if (Spurious.logging) {
                             * SpuriousLog.incEarlyTimeouts (); } }
                             */
                            while (state == waiting) {
                                delay = msecs - System.currentTimeMillis();
                                if (delay > Spurious.earlyTimeout) {
                                    if (Spurious.logging) {
                                        SpuriousLog.record(SpuriousLog.AlternativeSelectWithTimeout);
                                    }
                                    altMonitor.wait(delay);
                                } else {
                                    if ((delay > 0) && (Spurious.logging)) {
                                        SpuriousLog.incEarlyTimeouts();
                                    }
                                    break;
                                }
                            }
// System.out.println (state + " [" + delay + "]");
                        }
                    } else {
                        altMonitor.wait();
                        while (state == waiting) {
                            if (Spurious.logging) {
                                SpuriousLog.record(SpuriousLog.AlternativeSelect);
                            }
                            altMonitor.wait();
                        }
                    }
                } catch (InterruptedException e) {
                    throw new ProcessInterruptedException("*** Thrown from Alternative.priSelect ()\n" + e.toString());
                }
                state = ready;
            }
        }
        disableGuards();
        state = inactive;
        timeout = false;
        return selected;
    }

    /**
     * Returns the index of one of the ready guards. The method will block until one
     * of the guards becomes ready. Consequetive invocations will service the guards
     * `fairly' in the case when many guards are always ready. <I>Implementation
     * note: the last guard serviced has the lowest priority next time around.</I>
     */
    public final int fairSelect() {
        state = enabling;
        enableGuards();
        synchronized (altMonitor) {
            if (state == enabling) {
                state = waiting;
                try {
                    if (timeout) {
                        long delay = msecs - System.currentTimeMillis();
                        // NOTE: below is code that demonstrates whether wait (delay)
                        // sometimes returns early! Because this happens in some JVMs,
                        // we are forced into a workaround - see disableGuards ().
                        // long now = System.currentTimeMillis ();
                        // long delay = msecs - now;
                        if (delay > Spurious.earlyTimeout) {
                            altMonitor.wait(delay);
                            /*
                             * while ((state == waiting) && ((delay = msecs - System.currentTimeMillis ()) >
                             * Spurious.earlyTimeout)) { if (Spurious.logging) { SpuriousLog.record
                             * (SpuriousLog.AlternativeSelectWithTimeout); } altMonitor.wait (delay); } if
                             * ((state == waiting) && (delay > 0)) { if (Spurious.logging) {
                             * SpuriousLog.incEarlyTimeouts (); } }
                             */
                            while (state == waiting) {
                                delay = msecs - System.currentTimeMillis();
                                if (delay > Spurious.earlyTimeout) {
                                    if (Spurious.logging) {
                                        SpuriousLog.record(SpuriousLog.AlternativeSelectWithTimeout);
                                    }
                                    altMonitor.wait(delay);
                                } else {
                                    if ((delay > 0) && (Spurious.logging)) {
                                        SpuriousLog.incEarlyTimeouts();
                                    }
                                    break;
                                }
                            }
                        }
                        // long then = System.currentTimeMillis ();
                        // System.out.println ("*** fairSelect: " + msecs +
                        // ", " + now + ", " + delay +
                        // ", " + then);
                    } else {
                        altMonitor.wait();
                        while (state == waiting) {
                            if (Spurious.logging) {
                                SpuriousLog.record(SpuriousLog.AlternativeSelect);
                            }
                            altMonitor.wait();
                        }
                    }
                } catch (InterruptedException e) {
                    throw new ProcessInterruptedException("*** Thrown from Alternative.fairSelect/select ()\n"
                    + e.toString());
                }
                state = ready;
            }
        }
        disableGuards();
        state = inactive;
        favourite = selected + 1;
        if (favourite == guard.length)
            favourite = 0;
        timeout = false;
        return selected;
    }

    /**
     * Enables the guards for selection. If any of the guards are ready, it sets
     * selected to the ready guard's index, state to ready and returns. Otherwise,
     * it sets selected to NONE_SELECTED and returns.
     */
    private final void enableGuards() {
        if (barrierPresent) {
            // System.out.println ("ENABLE barrier(s) present ...");
            AltingBarrierCoordinate.startEnable();
        }
        barrierSelected = NONE_SELECTED;
        for (enableIndex = favourite; enableIndex < guard.length; enableIndex++) {
            if (guard[enableIndex].enable(this)) {
                // if (guard[enableIndex] instanceof AltingChannelInput) {
                // System.out.println ("CHANNEL ENABLE " + enableIndex + " SUCCEED");
                // } else {
                // System.out.println ("ENABLE " + enableIndex + " SUCCEED");
                // }
                selected = enableIndex;
                state = ready;
                if (barrierTrigger) {
                    barrierSelected = selected;
                    barrierTrigger = false;
                } else if (barrierPresent) {
                    // System.out.println ("ENABLE " + enableIndex + " NON-BARRIER SUCCEED");
                    AltingBarrierCoordinate.finishEnable();
                }
                return;
            } // else {
              // if (guard[enableIndex] instanceof AltingChannelInput) {
              // System.out.println ("CHANNEL ENABLE " + enableIndex + " FAIL");
            // } else {
            // System.out.println ("ENABLE " + enableIndex + " FAIL");
            // }
            // }
        }
        for (enableIndex = 0; enableIndex < favourite; enableIndex++) {
            if (guard[enableIndex].enable(this)) {
                // if (guard[enableIndex] instanceof AltingChannelInput) {
                // System.out.println ("CHANNEL ENABLE " + enableIndex + " SUCCEED");
                // } else {
                // System.out.println ("ENABLE " + enableIndex + " SUCCEED");
                // }
                selected = enableIndex;
                state = ready;
                if (barrierTrigger) {
                    barrierSelected = selected;
                    barrierTrigger = false;
                } else if (barrierPresent) {
                    // System.out.println ("ENABLE " + enableIndex + " NON-BARRIER SUCCEED");
                    AltingBarrierCoordinate.finishEnable();
                }
                return;
            } // else {
              // if (guard[enableIndex] instanceof AltingChannelInput) {
              // System.out.println ("CHANNEL ENABLE " + enableIndex + " FAIL");
            // } else {
            // System.out.println ("ENABLE " + enableIndex + " FAIL");
            // }
            // }
        }
        // System.out.println ("ENABLE ALL FAIL");
        selected = NONE_SELECTED;
        if (barrierPresent) {
            AltingBarrierCoordinate.finishEnable();
        }
    }

    /**
     * Disables the guards for selection. Sets selected to the index of the ready
     * guard, taking care of priority/fair choice.
     */
    private void disableGuards() {
        if (selected != favourite) { // else there is nothing to disable
            int startIndex = (selected == NONE_SELECTED) ? favourite - 1 : selected - 1;
            if (startIndex < favourite) {
                for (int i = startIndex; i >= 0; i--) {
                    if (guard[i].disable()) {
                        selected = i;
                        if (barrierTrigger) {
                            if (barrierSelected != NONE_SELECTED) {
                                throw new JCSP_InternalError("*** Second AltingBarrier completed in ALT sequence: "
                                + barrierSelected + " and " + i);
                            }
                            barrierSelected = selected;
                            barrierTrigger = false;
                        }
                    }
                }
                startIndex = guard.length - 1;
            }
            for (int i = startIndex; i >= favourite; i--) {
                if (guard[i].disable()) {
                    selected = i;
                    if (barrierTrigger) {
                        if (barrierSelected != NONE_SELECTED) {
                            throw new JCSP_InternalError("\n*** Second AltingBarrier completed in ALT sequence: "
                            + barrierSelected + " and " + i);
                        }
                        barrierSelected = selected;
                        barrierTrigger = false;
                    }
                }
            }
            if (selected == NONE_SELECTED) {
                // System.out.println ("disableGuards: NONE_SELECTED ==> " + timeIndex);
                // NOTE: this is a work-around for Java wait-with-timeouts sometimes
                // returning early. If this did not happen, we would not get here!
                selected = timeIndex;
            }
        }
        if (barrierSelected != NONE_SELECTED) { // We must choose a barrier sync
            selected = barrierSelected; // if one is ready - so that all
            AltingBarrierCoordinate.finishDisable(); // parties make the same choice.
        }
    }

    /**
     * This is the call-back from enabling a CSTimer guard. It is part of the
     * work-around for Java wait-with-timeouts sometimes returning early. It is
     * still in the flow of control of the ALTing process.
     */
    void setTimeout(long msecs) {
        if (timeout) {
            if (msecs < this.msecs) {
                this.msecs = msecs;
                timeIndex = enableIndex;
            }
        } else {
            timeout = true;
            this.msecs = msecs;
            timeIndex = enableIndex;
        }
    }

    /**
     * This is a call-back from an AltingBarrier. It is still in the flow of control
     * of the ALTing process.
     */
    void setBarrierTrigger() {
        barrierTrigger = true;
    }

    /**
     * This is the wake-up call to the process ALTing on guards controlled by this
     * object. It is in the flow of control of a process writing to an enabled
     * channel guard.
     */
    void schedule() {
        synchronized (altMonitor) {
            switch (state) {
            case enabling:
                state = ready;
                break;
            case waiting:
                state = ready;
                altMonitor.notify();
                break;
            // case ready: case inactive:
            // break
            }
        }
    }

    /////////////////// The pre-conditioned versions of select ///////////////////

    /**
     * Returns the index of one of the ready guards whose <code>preCondition</code>
     * index is true. The method will block until one of these guards becomes ready.
     * If more than one is ready, an <I>arbitrary</I> choice is made.
     * <P>
     * <I>Note: the length of the </I><code>preCondition</code><I> array must be the
     * same as that of the array of guards with which this object was
     * constructed.</I>
     * <P>
     *
     * @param preCondition the guards from which to select
     */
    public final int select(boolean[] preCondition) {
        return fairSelect(preCondition); // a legal implementation of arbitrary choice!
    }

    /**
     * Returns the index of one of the ready guards whose <code>preCondition</code>
     * index is true. The method will block until one of these guards becomes ready.
     * If more than one is ready, the one with the lowest index is selected.
     * <P>
     * <I>Note: the length of the </I><code>preCondition</code><I> array must be the
     * same as that of the array of guards with which this object was
     * constructed.</I>
     * <P>
     *
     * @param preCondition the guards from which to select
     */
    public final int priSelect(boolean[] preCondition) {
        // if (barrierPresent) {
        // throw new AlternativeError (
        // "*** Cannot 'priSelect' with an AltingBarrier in the Guard array"
        // );
        // }
        if (preCondition.length != guard.length) {
            throw new IllegalArgumentException("*** jcsp.lang.Alternative.select called with a preCondition array\n"
            + "*** whose length does not match its guard array");
        }
        state = enabling;
        favourite = 0;
        enableGuards(preCondition);
        synchronized (altMonitor) {
            if (state == enabling) {
                state = waiting;
                try {
                    if (timeout) {
                        long delay = msecs - System.currentTimeMillis();
                        if (delay > Spurious.earlyTimeout) {
                            altMonitor.wait(delay);
                            while (state == waiting) {
                                delay = msecs - System.currentTimeMillis();
                                if (delay > Spurious.earlyTimeout) {
                                    if (Spurious.logging) {
                                        SpuriousLog.record(SpuriousLog.AlternativeSelectWithTimeout);
                                    }
                                    altMonitor.wait(delay);
                                } else {
                                    if ((delay > 0) && (Spurious.logging)) {
                                        SpuriousLog.incEarlyTimeouts();
                                    }
                                    break;
                                }
                            }
                        }
                    } else {
                        altMonitor.wait();
                        while (state == waiting) {
                            if (Spurious.logging) {
                                SpuriousLog.record(SpuriousLog.AlternativeSelect);
                            }
                            altMonitor.wait();
                        }
                    }
                } catch (InterruptedException e) {
                    throw new ProcessInterruptedException("*** Thrown from Alternative.priSelect (boolean[])\n"
                    + e.toString());
                }
                state = ready;
            }
        }
        disableGuards(preCondition);
        state = inactive;
        timeout = false;
        return selected;
    }

    /**
     * Returns the index of one of the ready guards whose <code>preCondition</code>
     * index is true. The method will block until one of these guards becomes ready.
     * Consequetive invocations will service the guards `fairly' in the case when
     * many guards are always ready. <I>Implementation note: the last guard serviced
     * has the lowest priority next time around.</I>
     * <P>
     * <I>Note: the length of the </I><code>preCondition</code><I> array must be the
     * same as that of the array of guards with which this object was
     * constructed.</I>
     * <P>
     *
     * @param preCondition the guards from which to select
     */
    public final int fairSelect(boolean[] preCondition) {
        if (preCondition.length != guard.length) {
            throw new IllegalArgumentException("*** jcsp.lang.Alternative.select called with a preCondition array\n"
            + "*** whose length does not match its guard array");
        }
        state = enabling;
        enableGuards(preCondition);
        synchronized (altMonitor) {
            if (state == enabling) {
                state = waiting;
                try {
                    if (timeout) {
                        long delay = msecs - System.currentTimeMillis();
                        if (delay > Spurious.earlyTimeout) {
                            altMonitor.wait(delay);
                            while (state == waiting) {
                                delay = msecs - System.currentTimeMillis();
                                if (delay > Spurious.earlyTimeout) {
                                    if (Spurious.logging) {
                                        SpuriousLog.record(SpuriousLog.AlternativeSelectWithTimeout);
                                    }
                                    altMonitor.wait(delay);
                                } else {
                                    if ((delay > 0) && (Spurious.logging)) {
                                        SpuriousLog.incEarlyTimeouts();
                                    }
                                    break;
                                }
                            }
                        }
                    } else {
                        altMonitor.wait();
                        while (state == waiting) {
                            if (Spurious.logging) {
                                SpuriousLog.record(SpuriousLog.AlternativeSelect);
                            }
                            altMonitor.wait();
                        }
                    }
                } catch (InterruptedException e) {
                    throw new ProcessInterruptedException("*** Thrown from Alternative.fairSelect/select (boolean[])\n"
                    + e.toString());
                }
                state = ready;
            }
        }
        disableGuards(preCondition);
        state = inactive;
        favourite = selected + 1;
        if (favourite == guard.length)
            favourite = 0;
        timeout = false;
        return selected;
    }

    /**
     * Enables the guards for selection. The preCondition must be true for an guard
     * to be selectable. If any of the guards are ready, it sets selected to the
     * ready guard's index, state to ready and returns. Otherwise, it sets selected
     * to NONE_SELECTED and returns.
     * <P>
     *
     * @return true if and only if one of the guards is ready
     */
    private final void enableGuards(boolean[] preCondition) {
        if (barrierPresent) {
            AltingBarrierCoordinate.startEnable();
        }
        barrierSelected = NONE_SELECTED;
        for (enableIndex = favourite; enableIndex < guard.length; enableIndex++) {
            if (preCondition[enableIndex] && guard[enableIndex].enable(this)) {
                selected = enableIndex;
                state = ready;
                if (barrierTrigger) {
                    barrierSelected = selected;
                    barrierTrigger = false;
                } else if (barrierPresent) {
                    AltingBarrierCoordinate.finishEnable();
                }
                return;
            }
        }
        for (enableIndex = 0; enableIndex < favourite; enableIndex++) {
            if (preCondition[enableIndex] && guard[enableIndex].enable(this)) {
                selected = enableIndex;
                state = ready;
                if (barrierTrigger) {
                    barrierSelected = selected;
                    barrierTrigger = false;
                } else if (barrierPresent) {
                    AltingBarrierCoordinate.finishEnable();
                }
                return;
            }
        }
        selected = NONE_SELECTED;
        if (barrierPresent) {
            AltingBarrierCoordinate.finishEnable();
        }
    }

    /**
     * Disables the guards for selection. The preCondition must be true for an guard
     * to be selectable. Sets selected to the index of the ready guard, taking care
     * of priority/fair choice.
     */
    private void disableGuards(boolean[] preCondition) {
        if (selected != favourite) { // else there is nothing to disable
            int startIndex = (selected == NONE_SELECTED) ? favourite - 1 : selected - 1;
            if (startIndex < favourite) {
                for (int i = startIndex; i >= 0; i--) {
                    if (preCondition[i] && guard[i].disable()) {
                        selected = i;
                        if (barrierTrigger) {
                            if (barrierSelected != NONE_SELECTED) {
                                throw new JCSP_InternalError("*** Second AltingBarrier completed in ALT sequence: "
                                + barrierSelected + " and " + i);
                            }
                            barrierSelected = selected;
                            barrierTrigger = false;
                        }
                    }
                }
                startIndex = guard.length - 1;
            }
            for (int i = startIndex; i >= favourite; i--) {
                if (preCondition[i] && guard[i].disable()) {
                    selected = i;
                    if (barrierTrigger) {
                        if (barrierSelected != NONE_SELECTED) {
                            throw new JCSP_InternalError("*** Second AltingBarrier completed in ALT sequence: "
                            + barrierSelected + " and " + i);
                        }
                        barrierSelected = selected;
                        barrierTrigger = false;
                    }
                }
            }
            if (selected == NONE_SELECTED) {
                // System.out.println ("disableGuards: NONE_SELECTED ==> " + timeIndex);
                // NOTE: this is a work-around for Java wait-with-timeouts sometimes
                // returning early. If this did not happen, we would not get here!
                selected = timeIndex;
            }
        }
        if (barrierSelected != NONE_SELECTED) { // We must choose a barrier sync
            selected = barrierSelected; // if one is ready - so that all
            AltingBarrierCoordinate.finishDisable(); // parties make the same choice.
        }
    }

}
