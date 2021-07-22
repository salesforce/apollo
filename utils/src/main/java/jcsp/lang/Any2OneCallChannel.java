
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
 * This is the super-class for any-to-one <TT>interface</TT>-specific CALL
 * channels, safe for use by many clients and one server.
 * <P>
 * <A HREF="#constructor_summary">Shortcut to the Constructor and Method
 * Summaries.</A>
 *
 * <H2>Description</H2> Please see {@link One2OneCallChannel} for general
 * information about CALL channels. Documented here is information specific to
 * this <I>any-1</I> version.
 *
 * <H3><A NAME="Convert">Converting a Method Interface into a Variant CALL
 * Channel</H3> Constructing a <I>any-1</I> CALL channel for a specific
 * <TT>interface</TT> follows exactly the same pattern as in the <I>1-1</I>
 * case. Of course, it must extend <TT>Any2OneCallChannel</TT> rather than
 * <TT>One2OneCallChannel</TT>.
 * <P>
 * For example, using the same
 * <A HREF="One2OneCallChannel.html#Foo"><TT>Foo</TT></A> interface as before,
 * we derive:
 * 
 * <PRE>
 * import jcsp.lang.*;
 * <I></I>
 * public class Any2OneFooChannel extends Any2OneCallChannel implements Foo {
 * <I></I>
 *   ...  same body as <A HREF=
"One2OneCallChannel.html#One2OneFooChannel"><TT>One2OneFooChannel</TT></A>
 * <I></I>
 * }
 * </PRE>
 *
 * <H3><A NAME="Call">Calling a CALL Channel</H3> All the <I>client</I> needs to
 * see is the method <TT>interface</TT> implemented by the CALL channel. So far
 * as the <I>client</I> is concerned, therefore, there is <I>no</I> difference
 * between any of the varieties of CALL channel - it just
 * <A HREF="One2OneCallChannel.html#Call">makes the call</A>.
 *
 * <H3><A NAME="Accept">Accepting a CALL Channel</H3> The mechanics of accepting
 * a CALL channel are the same for all varieties. However, the <I>server</I>
 * should declare which kind (or kinds) it allows to be attached:
 * 
 * <PRE>
 * import jcsp.lang.*;
 * <I></I>
 * class B implements CSProcess, Foo {
 * <I></I>
 *   private final ChannelAccept in;
 * <I></I>
 *   public B (final One2OneFooChannel in) {         // original constructor
 *     this.in = in;
 *   }
 * <I></I>
 *   public B (final Any2OneFooChannel in) {        // additional constructor
 *     this.in = in;
 *   }
 * <I></I>
 *   ...  rest <A HREF="One2OneCallChannel.html#Accept">as before</A>
 * <I></I>
 * }
 * </PRE>
 * 
 * When wrapping the above to hide its raw method interface, don't forget to
 * include the extra constructor(s):
 * 
 * <PRE>
 * import jcsp.lang.*;
 * <I></I>
 * public class B2 implements CSProcess {            // no Foo interface
 * <I></I>
 *   private final B b;
 * <I></I>
 *   public B2 (final One2OneFooChannel in) {        // original constructor
 *     b = new B (in);
 *   }
 * <I></I>
 *   public B2 (final Any2OneFooChannel in) {       // additional constructor
 *     b = new B (in);
 *   }
 * <I></I>
 *   public void run () {
 *     b.run ();
 *   }
 * <I></I>
 * }
 * </PRE>
 *
 * <H3><A NAME="ALTing">ALTing on a CALL Channel</H3> The <I>server</I> may ALT
 * on a <I>any-1</I> CALL channel, just as it may ALT on a <I>1-1</I> one. As
 * before, it needs to make its intentions explicit. So, in the
 * <A HREF="#Accept">above example</A>, the first field declaration of
 * <TT>B</TT> needs to become:
 * 
 * <PRE>
 * private final AltingChannelAccept in;
 * </PRE>
 * 
 * See <A HREF="#Example">below</A> for an example of <TT>ALT</TT>ing between
 * CALL channels.
 *
 * <H3><A NAME="Network">Building a CALL Channel Network</H3> Network building
 * with CALL channels is the same as building with <I>ordinary</I> channels.
 * First construct the channels and, then, construct the processes - plugging in
 * the channels as required and running them in {@link Parallel}.
 * <P>
 * For example, the network consisting of one <I>server</I> and several
 * <I>clients</I>:
 * <p>
 * <IMG SRC="doc-files\Any2OneCallChannel1.gif">
 * </p>
 * where <TT>A</TT> is unchanged from its definition in
 * <A HREF="One2OneCallChannel.html#Call"><TT>One2OneCallChannel</TT></A>, is
 * implemented by:
 * 
 * <PRE>
 *     Any2OneFooChannel c = new Any2OneFooChannel ();
 * <I></I>
 *     final A[] aClients = new A[n_aClients];
 *     for (int i = 0; i < aClients.length; i++) {
 *       aClients[i] = new A (c);
 *     }
 * <I></I>
 *     new Parallel (
 *       new CSProcess[] {
 *         new Parallel (aClients),
 *         new B2 (c)
 *       }
 *     ).run ();
 * </PRE>
 *
 * <H2><A NAME="Example">Example</H2> This is a CALL channel version of the
 * <A HREF="Alternative.html#Wot-no-Chickens"><I>Wot-no-Chickens</I></A>
 * example:
 * <p>
 * <IMG SRC="doc-files\Any2OneCallChannel2.gif">
 * </p>
 * The <TT>service</TT> CALL channel replaces the
 * <TT>request</TT>/<TT>deliver</TT> channel pair of the earlier example.
 * Previously, the philosopher had to perform two actions to get a chicken - a
 * <TT>request.write</TT> followed by a <TT>deliver.read</TT>. Now, its
 * interaction with the canteen is a single CALL on
 * <TT>service.takeChicken</TT>.
 * <P>
 * The <TT>supply</TT> CALL channel replaces the <I>ordinary</I> channel of the
 * same name. Previously, the chef still had to perform two actions to supply
 * the chickens - a <TT>supply.write</TT> followed by a second
 * <TT>supply.write</TT>. This was to model the extended period while the chef
 * set down the chickens in the canteen. The first communication synchronised
 * the chef with the canteen, getting its exclusive attention. The canteen then
 * executed the set-down delay before accepting the second communication and,
 * hence, releasing the chef. Now, this interaction is a single CALL on
 * <TT>supply.freshChickens</TT>.
 * <P>
 * The other difference with the earlier example is that the college now employs
 * many chefs. This has two minor impacts. It needs to be able to support
 * <I>any-1</I> CALLs on its <TT>supply</TT> channel (as well as on
 * <TT>service</TT>). Secondly, with all those chefs, it needs to be able to
 * <I>refuse</I> further supplies of chicken if it has run of room.
 *
 * <H3><A NAME="Canteen">The Canteen</H3> There are two other differences in
 * design between the canteen <I>server</I> here and the CALL channel servers
 * documented above and in {@link One2OneCallChannel}. The first is trivial - we
 * have inlined the <I>real</I> server as an anonymous inner class of the public
 * <TT>Canteen</TT> wrapper. The second is more subtle, but also trivial. Often,
 * a CALL channel is constructed for a <I>specific</I> server interface and
 * there is no intention for it to be used for communicating with any other
 * server. In which case, it makes sense to tie that interface, together with
 * its corresponding CALL channel, into the server as inner declarations.
 * <P>
 * So, the Canteen first publishes its two specific interfaces and matching CALL
 * channels. The CALL channels follow the
 * <A HREF="One2OneCallChannel.html#One2OneFooChannel">defined pattern</A>,
 * omitting the optional setting of <TT>selected</TT> (since each interface
 * contains only one method):
 * 
 * <PRE>
 * import jcsp.lang.*;
 * <I></I>
 * class Canteen implements CSProcess {
 * <I></I>
 *   public static interface Service {
 *     public int takeChicken (String philId);
 *   }
 * <I></I>
 *   public static class One2OneServiceChannel
 *    extends One2OneCallChannel implements Service {
 *     public int takeChicken (String philId) {
 *       join ();
 *       int n = ((Service) server).takeChicken (philId);
 *       fork ();
 *       return n;
 *     }
 *   }
 * <I></I>
 *   public static class Any2OneServiceChannel
 *    extends Any2OneCallChannel implements Service {
 *     public int takeChicken (String philId) {
 *       join ();
 *       int n = ((Service) server).takeChicken (philId);
 *       fork ();
 *       return n;
 *     }
 *   }
 * <I></I>
 *   public static interface Supply {
 *     public int freshChickens (String chefId, int value);
 *   }
 * <I></I>
 *   public static class Any2OneSupplyChannel
 *    extends Any2OneCallChannel implements Supply {
 *     public int freshChickens (String chefId, int value) {
 *       join ();
 *       int n = ((Supply) server).freshChickens (chefId, value);
 *       fork ();
 *       return n;
 *     }
 *   }
 * </PRE>
 * 
 * Note that we have defined both <I>1-1</I> and <I>any-1</I> versions of the
 * <TT>Service</TT> CALL channel. This example makes use only of the
 * <I>any-1</I> variant - the other will be used in a
 * <A HREF="Any2AnyCallChannel.html#Student">later</A> exercise.
 * <P>
 * Next we set up the constructor and the local fields for saving its
 * parameters:
 * 
 * <PRE>
 *   private final AltingChannelAccept service;   // shared from all Philosphers
 *   private final AltingChannelAccept supply;    // shared from all Chefs
 *   private final int serviceTime;
 *   // how long a philosopher spends in the canteen
 *   private final int supplyTime;
 *   // how long a chef spends in the canteen
 *   private final int maxChickens;
 *   // maximum number of chickens in the canteen
 * <I></I>
 *   public Canteen (Any2OneServiceChannel service, Any2OneSupplyChannel supply,
 *                   int serviceTime, int supplyTime, int maxChickens) {
 *     this.service = service;
 *     this.supply = supply;
 *     this.serviceTime = serviceTime;
 *     this.supplyTime = supplyTime;
 *     this.maxChickens = maxChickens;
 *   }
 * </PRE>
 * 
 * Now, we need to combine the exported interfaces into a single one so that the
 * inner process can be created (anonymously) by this wrapper's run method:
 * 
 * <PRE>
 *   private interface inner extends CSProcess, Service, Supply {};
 * <I></I>
 *   public void run () {
 * <I></I>
 *     new inner () {
 * <I></I>
 *       private int nChickens = 0;
 *       private int nSupplied = 0;
 * <I></I>
 *       private final CSTimer tim = new CSTimer ();
 * </PRE>
 * 
 * Impementations of the required CALL interfaces come next:
 * 
 * <PRE>
 *       public int takeChicken (String philId) {
 *       // pre : nChickens > 0
 *         System.out.println ("   Canteen -> " + philId
 *                             + " : one chicken ordered ... "
 *                             + nChickens + " left ... ");
 *         tim.sleep (serviceTime);
 *         nChickens--;
 *         nSupplied++;
 *         System.out.println ("   Canteen -> " + philId
 *                             + " : one chicken coming down ... "
 *                             + nChickens + " left ... ["
 *                             + nSupplied + " supplied]");
 *         return 1;
 *       }
 * <I></I>
 *       public int freshChickens (String chefId, int value) {
 *       // pre : nChickens < maxChickens
 *         System.out.println ("   Canteen <- " + chefId
 *                             + " : ouch ... make room ... ");
 *         tim.sleep (supplyTime);
 *         nChickens += value;
 *         int sendBack = nChickens - maxChickens;
 *         if (sendBack > 0) {
 *           nChickens = maxChickens;
 *           System.out.println ("   Canteen <- " + chefId
 *                               + " : full up ... sending back "
 *                               + sendBack);
 *         } else {
 *           sendBack = 0;
 *         }
 *         System.out.println ("   Canteen <- " + chefId
 *                             + " : more chickens ... "
 *                             + nChickens + " now available ... ");
 *         return sendBack;
 *       }
 * </PRE>
 * 
 * and the run method that conducts everything:
 * 
 * <PRE>
 *       public void run () {
 * <I></I>
 *         final Alternative alt = new Alternative (new Guard[] {supply, service});
 *         final boolean[] precondition = {true, false};
 *         final int SUPPLY = 0;
 *         final int SERVICE = 1;
 * <I></I>
 *         System.out.println ("   Canteen : starting ... ");
 *         while (true) {
 *           precondition[SERVICE] = (nChickens > 0);
 *           precondition[SUPPLY] = (nChickens < maxChickens);
 *           switch (alt.fairSelect (precondition)) {
 *             case SUPPLY:
 *               supply.accept (this);      // new batch of chickens from a chef
 *             break;
 *             case SERVICE:
 *               service.accept (this);     // a philosopher wants a chicken
 *             break;
 *           }
 *         }
 * <I></I>
 *       }
 * </PRE>
 * 
 * Finally, don't forget to run this inner process:
 * 
 * <PRE>
 *     }.run ();
 * <I></I>
 *   }
 * <I></I>
 * }
 * </PRE>
 *
 * <H3><A NAME="Philosopher">The Philosopher</H3> As in the original example,
 * philosophers spend their time thinking, feeling hungry, calling on the
 * canteen and, once served, eating. Except, of course, for greedy philosophers
 * who never stop to think:
 * 
 * <PRE>
 * import jcsp.lang.*;
 * <I></I>
 * class Phil implements CSProcess {
 * <I></I>
 *   private final String id;
 *   private final Canteen.Service service;
 *   private final int thinkTime;
 *   private final int eatTime;
 *   private final boolean greedy;
 * <I></I>
 *   public Phil (String id, Canteen.Service service,
 *                int thinkTime, int eatTime, boolean greedy) {
 *     this.id = id;
 *     this.service = service;
 *     this.thinkTime = thinkTime;
 *     this.eatTime = eatTime;
 *     this.greedy = greedy;
 *   }
 * <I></I>
 *   public void run () {
 *     final CSTimer tim = new CSTimer ();
 *     int nEaten = 0;
 *     while (true) {
 *       if (! greedy) {
 *         System.out.println ("   Phil " + id
 *                             + " : thinking ... ");
 *         tim.sleep (thinkTime);  // thinking
 *       }
 *       System.out.println ("   Phil " + id
 *                           + " : gotta eat ... ");
 *       int chicken = service.takeChicken (id);
 *       nEaten++;
 *       System.out.println ("   Phil " + id
 *                           + " : mmm ... that's good ["
 *                           + nEaten + " so far]");
 *       tim.sleep (eatTime);      // eating
 *     }
 *   }
 * <I></I>
 * }
 * </PRE>
 *
 * <H3><A NAME="Chef">The Chef</H3> Chefs cook chickens in batches of
 * <TT>batchSize</TT>, taking <TT>batchTime</TT> milliseconds per batch. When a
 * batch is ready, the chef supplies it to the canteen. The chef has to wait
 * until the canteen is prepared to take it and, then, helps to set down the
 * batch (before returning with any for which there was no space) - all this
 * happens during the CALL of <TT>supply.freshChickens</TT>:
 * 
 * <PRE>
 * import jcsp.lang.*;
 * <I></I>
 * class Chef implements CSProcess {
 * <I></I>
 *   private final String id;
 *   private final int batchSize;
 *   private final int batchTime;
 *   private final Canteen.Supply supply;
 * <I></I>
 *   public Chef (String id, int batchSize, int batchTime, Canteen.Supply supply) {
 *     this.id = id;
 *     this.batchSize = batchSize;
 *     this.batchTime = batchTime;
 *     this.supply = supply;
 *   }
 * <I></I>
 *   public void run () {
 * <I></I>
 *     final CSTimer tim = new CSTimer ();
 * <I></I>
 *     int nReturned = 0;
 *     int nSupplied = 0;
 * <I></I>
 *     while (true) {
 *       System.out.println ("   Chef " + id + " : cooking ... "
 *                           + (batchSize - nReturned) + " chickens");
 *       tim.sleep (batchTime);
 *       System.out.println ("   Chef " + id + " : "
 *                           + batchSize + " chickens, ready-to-go ... ");
 *       nReturned = supply.freshChickens (id, batchSize);
 *       nSupplied += (batchSize - nReturned);
 *       System.out.println ("   Chef " + id + " : "
 *                           + nReturned + " returned ["
 *                           + nSupplied + " supplied]");
 *     }
 *   }
 * <I></I>
 * }
 * </PRE>
 *
 * <H3><A NAME="Clock">The Clock</H3> The college is feeling generous and
 * provides a clock. This just ticks away, delivering time-stamps roughly every
 * second (and maintaining real-time). It is independent of the rest of the
 * system.
 * 
 * <PRE>
 * import jcsp.lang.*;
 * <I></I>
 * class Clock implements CSProcess {
 * <I></I>
 *   public void run () {
 * <I></I>
 *     final CSTimer tim = new CSTimer ();
 *     final long startTime = tim.read ();
 * <I></I>
 *     while (true) {
 *       int tick = (int) (((tim.read () - startTime) + 500)/1000);
 *       System.out.println ("[TICK] " + tick);
 *       tim.sleep (1000);
 *     }
 * <I></I>
 *   }
 * <I></I>
 * }
 * </PRE>
 *
 * <H3><A NAME="College">The College</H3> Despite the greedy behaviour of
 * philosopher 0 (<I>Bill</I>), nobody starves in this college. Three chefs are
 * provided with differing cooking speeds and batch sizes. <I>Pierre</I> is the
 * original lightning chef, cooking 4 chickens in 2 seconds flat. <I>Henri</I>
 * is more leisurely, taking 20 seconds to cook his batch of 10. <I>Sid</I> has
 * been sent down by the new owners of the college, who are into mass catering.
 * He produces 100 chickens every 150 seconds, which is a bit silly since the
 * canteen has only space for 50. Still, it enables <I>Bill</I> to get really
 * sick!
 * <P>
 * For convenience, the college network diagram is reproduced here - this time
 * including the clock and naming some of the characters:
 * <p>
 * <IMG SRC="doc-files\Any2OneCallChannel3.gif">
 * </p>
 * Here is the code:
 * 
 * <PRE>
 * import jcsp.lang.*;
 * <I></I>
 * class College implements CSProcess {
 * <I></I>
 *   public void run () {
 * <I></I>
 *     final String[] philId = {"Bill", "Hilary", "Gennifer", "Paula", "Monica"};
 * <I></I>
 *     final int thinkTime = 3000;             // 3 seconds
 *     final int eatTime = 100;                // 100 milliseconds
 * <I></I>
 *     final int serviceTime = 0;              // 0 seconds
 *     final int supplyTime = 3000;            // 3 seconds
 *     final int maxChickens = 50;
 * <I></I>
 *     final Canteen.Any2OneServiceChannel service
 *       = new Canteen.Any2OneServiceChannel ();
 *     final Canteen.Any2OneSupplyChannel supply
 *       = new Canteen.Any2OneSupplyChannel ();
 * <I></I>
 *     final Phil[] phils = new Phil[philId.length];
 *     for (int i = 0; i < phils.length; i++) {
 *       phils[i] = new Phil (philId[i], service, thinkTime, eatTime, i == 0);
 *     }
 * <I></I>
 *     new Parallel (
 *       new CSProcess[] {
 *         new Clock (),
 *         new Canteen (service, supply, serviceTime, supplyTime, maxChickens),
 *         new Parallel (phils),
 *         new Chef ("Pierre", 4, 2000, supply),
 *         // chefId, batchSize, batchTime
 *         new Chef ("Henri", 10, 20000, supply),
 *         new Chef ("Sid", 100, 150000, supply)
 *       }
 *     ).run ();
 * <I></I>
 *   }
 * <I></I>
 *   public static void main (String argv[]) {
 *     new College ().run ();
 *   }
 * <I></I>
 * }
 * </PRE>
 *
 * <H3><A NAME="System.out">A Note about <TT>System.out</TT> and Other
 * Non-Blocking Monitors</H3> The college <A HREF="#College">network diagram and
 * code</A> hides a key player that quietly coordinates the reporting of all
 * activity - <TT>System.out</TT>. All the processes share this object as a
 * common resource, making heavy and concurrent demands on its <TT>println</TT>
 * service. Why is this safe?
 * <P>
 * Consider a Java object whose public methods are all <TT>synchronized</TT> but
 * contain no invocations of <TT>wait</TT> or <TT>notify</TT> (a passive
 * <I>non-blocking monitor</I>). Such an object is equivalent to a
 * <TT>CSProcess</TT> serving one or more <I>any-1</I> CALL channels (whose
 * interfaces reflect those <TT>synchronized</TT> methods) and whose
 * <TT>run</TT> consists of an endless loop that does nothing except
 * unconditionally <TT>accept</TT> any CALL.
 * <P>
 * So, a simple non-blocking monitor is always safe to share between concurrent
 * JCSP processes and, currently, carries less overheads than its active
 * <I>server</I> equivalent. See {@link jcsp.awt.DisplayList} for an example
 * from the JCSP library. Another example is {@link java.io.PrintStream
 * <TT>java.io.PrintStream</TT>}, of which <TT>System.out</TT> is an instance.
 * Its <TT>print</TT>/<TT>println</TT> methods are <TT>synchronized</TT> on
 * itself (although this does not seem to be documented and you have to look
 * hard at the code to find out). So, to show the full story, the above
 * <A HREF="#College">diagram</A> possibly needs an overlay that adds a
 * <I>System.out</I> process servicing a any-1 <I>println</I> CALL channel, with
 * all the other processes as clients. This is left as an exercise.
 * <P>
 * Of course, it would be nice if such monitors were accessed via an
 * <TT>interface</TT>, so that client processes had neither direct visibilty of
 * them nor concern about their behaviour. A problem with the above college is
 * that <TT>System.out</TT> - and the concept of printing a line of text - is
 * burnt into the code of all its processes. If we wanted to change the output
 * of the college from a scrolling text display into some graphics animation,
 * all those processes would have to be changed.
 * <P>
 * A better design would pass in channel (or CALL channel or <I>non-blocking</I>
 * monitor) interfaces to each of the college processes. These would merely
 * report their identities and states by writing to (or calling or invoking)
 * those interfaces. To reproduce the current display, all those interfaces
 * would be instanced by a single <I>any-1</I> channel (or CALL channel or
 * monitor) connected to a simple server that responds by making
 * <TT>System.out.println</TT> invocations appropriate to the information
 * passed. For other effects, connect in other servers. Note that the college
 * processes do not have to be connected to the same server - each could be
 * connected to a separate server and these servers connected into a graphics
 * animation network (incorporating, for example, processes from
 * {@link jcsp.awt}). The point is that the college processes would need no
 * changing to drive whatever was constructed. This is also left as an exercise.
 *
 * <H3><A NAME="MODULE">Further Thoughts on the Canteen Design</H3> The decision
 * to bind in the CALL channel (and associated inteface) definitions as inner
 * classes of <TT>Canteen</TT> does not fit comfortably with the above
 * observations. However, processes servicing
 * <TT>Canteen.Any2OneServiceChannel</TT> or
 * <TT>Canteen.Any2OneSupplyChannel</TT> channels do not have to be instances of
 * <TT>Canteen</TT> - even though that would seem to be a little odd. So, the
 * <TT>College</TT> authorities still have freedom to install canteens with
 * behaviours quite different to that of <TT>Canteen</TT>. [Anything with the
 * same channel interface will fit! The interoperability of processes depends
 * only on the compatibility of their channel interfaces. Note that this
 * reusability owes nothing to the concept of <I>inheritance</I> - for example,
 * the alternative canteens pluggable into the college network need no special
 * sub-classing relationships.] So, if we really want to allow this flexibility,
 * make it explicit by declaring the CALL channels separately from any of their
 * servers.
 * <P>
 * Another design choice is to burn in CALL channel instances as part of the
 * servers themselves. For example, the <TT>Canteen</TT> class could construct
 * and export its <TT>service</TT> and <TT>supply</TT> channels as
 * <TT>public</TT> and <TT>final</TT> fields (rather than import them via
 * constructor parameters). In this case, the <TT>College</TT> builder would
 * need to declare <I>and name</I> the canteen (instead of declaring and naming
 * the channels):
 * 
 * <PRE>
 * import jcsp.lang.*;
 * <I></I>
 * class College implements CSProcess {
 * <I></I>
 *   public void run () {
 * <I></I>
 *     ...  declare constants (nPhilosophers, thinkTime etc.)
 * <I></I>
 *     final Canteen canteen = new Canteen (serviceTime, supplyTime,
 *                                          maxChickens);
 * <I></I>
 *     final Phil[] phils = new Phil[nPhilosophers];
 *     for (int i = 0; i < phils.length; i++) {
 *       String philId = new Integer (i).toString ();
 *       phils[i] = new Phil (philId, canteen.service,
 *                            thinkTime, eatTime, i == 0);
 *     }
 * <I></I>
 *     new Parallel (
 *       new CSProcess[] {
 *         new Clock (),
 *         canteen,
 *         new Parallel (phils),
 *         new Chef ("Pierre", 4, 2000, canteen.supply),
 *         new Chef ("Henri", 10, 20000, canteen.supply),
 *         new Chef ("Sid", 100, 60000, canteen.supply)
 *       }
 *     ).run ();
 *   }
 *   ...  main
 * }
 * </PRE>
 * 
 * Note that this particular <I>burn in</I> does not deny any flexibility to the
 * college in choosing any particular variety of canteen. In fact, the only
 * thing of interest to the college is that the canteen provides and services
 * CALL channels whose interfaces are what its philosophers and chefs expect
 * (i.e. <TT>Service</TT> and <TT>Supply</TT>). Note also that the college
 * <A HREF="#College">network diagram</A> has not changed.
 * <P>
 * Having gone this far, we may like to consider making the server
 * <I>self-starting</I> - so that its declaration not only introduces its
 * service channels but also brings it to life. For example, this could be done
 * for the <TT>Canteen</TT> by adding the following as the last line of its
 * constructor:
 * 
 * <PRE>
 *     new {@link ProcessManager} (this).start ();
 * </PRE>
 * 
 * Of course, the <TT>canteen</TT> instance should then be removed from the
 * {@link Parallel} construction above.
 * <P>
 * [<I>Warning:</I> be careful if sub-classes are allowed (i.e. the
 * <TT>Canteen</TT> class was not declared <TT>final</TT>). In this case, the
 * above incantation should be optional so that each sub-class constructor can
 * invoke a super-class constructor that omits it. If we let the super-class
 * fire up the process, it may start running before the sub-class constructor
 * finishes - i.e. before the process has been fully initialised. It must be the
 * sub-class constructor that self-starts the process (as the last thing it
 * does).]
 * <P>
 * [<I>Note:</I> a self-starting server exporting its own CALL (or ordinary)
 * channels for public concurrent use corresponds to the <B>occam3</B> notion of
 * a <TT>MODULE</TT> implemented by a <TT>RESOURCE</TT>.]
 *
 * @see One2OneCallChannel
 * @see jcsp.lang.One2AnyCallChannel
 * @see Any2AnyCallChannel
 * @see Alternative
 *
 * @author P.H. Welch
 */

public abstract class Any2OneCallChannel extends AltingChannelAccept implements Serializable {
    private static final long        serialVersionUID = 1L;
    /**
     * This is used to synchronise the calling and accepting process.
     */
    final private Any2OneChannelImpl c                = new Any2OneChannelImpl();

    /**
     * This is used to synchronise the calling and accepting process.
     */
    final private One2OneChannelImpl d = new One2OneChannelImpl();

    /**
     * This holds a reference to a <I>server</I> process so that a <I>client</I> may
     * make the call. The reference is only valid between the {@link #join
     * <TT>join</TT>} and {@link #fork <TT>fork</TT>} elements of the standard
     * <A HREF="One2OneCallChannel.html#One2OneFooChannel">calling sequence</A>. As
     * shown in that sequence, it will need casting up to the relevant interface
     * supported by the specific CALL channel derived from this class.
     */
    protected CSProcess server; // made available to the caller

    /**
     * This may be set during the standard
     * <A HREF="One2OneCallChannel.html#One2OneFooChannel">calling sequence</A> to
     * record which method was invoked by a <I>client</I>. It is only safe to do
     * this between the {@link #join <TT>join</TT>} and {@link #fork <TT>fork</TT>}
     * elements of that sequence. Either <I>all</I> the CALL channel methods should
     * do this or <I>none</I> - in the latter case, its default value remains as
     * zero. Its value is returned to a <I>server</I> as the result the
     * <I>server</I>'s invocation of {@link #accept <TT>accept</TT>}.
     */
    protected int selected = 0; // set (optionally) by the caller

    /**
     * This is invoked by a <I>server</I> when it commits to accepting a CALL from a
     * <I>client</I>. The parameter supplied must be a reference to this
     * <I>server</I> - see the <A HREF="One2OneCallChannel.html#Accept">example</A>
     * from {@link One2OneCallChannel}. It will not complete until a CALL has been
     * made. If the derived CALL channel has set the {@link #selected} field in the
     * way defined by the standard
     * <A HREF="One2OneCallChannel.html#One2OneFooChannel">calling sequence</A>, the
     * value returned by this method will indicate which method was called.
     *
     * @param server the <I>server</I> process receiving the CALL.
     */
    @Override
    public int accept(CSProcess server) {
        // invoked by the callee
        this.server = server;
        c.read(); // ready to ACCEPT the CALL
        d.read(); // wait until the CALL is complete
        return selected;
    }

    /**
     * This is invoked by a <I>client</I> during the standard
     * <A HREF="One2OneCallChannel.html#One2OneFooChannel">calling sequence</A>. It
     * will not complete until a <I>server</I> invokes an {@link #accept
     * <TT>accept</TT>} on this channel. In turn, that <TT>accept</TT> will not
     * complete until the <I>client</I> invokes a {@link #fork <TT>fork</TT>}, after
     * having made its CALL on the <I>server</I>.
     */
    protected void join() {
        // indirectly invoked by the caller
        c.write(null);
    }

    /**
     * This is invoked by a <I>client</I> during the standard
     * <A HREF="One2OneCallChannel.html#One2OneFooChannel">calling sequence</A>. A
     * <I>server</I> must have invoked an {@link #accept <TT>accept</TT>} for the
     * <I>client</I> to have got this far in the sequence - see the {@link #join
     * <TT>join</TT>}. This call unblocks that <TT>accept</TT>, releasing the
     * <I>server</I> and <I>client</I> to resume separate lives.
     */
    protected void fork() {
        // indirectly invoked by the caller
        d.write(null);
    }

    /**
     * This is one of the {@link Guard} methods needed by the {@link Alternative}
     * class.
     */
    @Override
    boolean enable(Alternative alt) {
        // ignore this!
        return c.readerEnable(alt);
    }

    /**
     * This is one of the {@link Guard} methods needed by the {@link Alternative}
     * class.
     */
    @Override
    boolean disable() {
        // ignore this!
        return c.readerDisable();
    }
}
