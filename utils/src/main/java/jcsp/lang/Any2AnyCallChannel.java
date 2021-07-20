
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
 * This is the super-class for any-to-any <TT>interface</TT>-specific CALL
 * channels, safe for use by many clients and many servers.
 * <P>
 * <A HREF="#constructor_summary">Shortcut to the Constructor and Method
 * Summaries.</A>
 *
 * <H2>Description</H2> Please see {@link One2OneCallChannel} for general
 * information about CALL channels. Documented here is information specific to
 * this <I>any-any</I> version.
 *
 * <H3><A NAME="Convert">Converting a Method Interface into a Variant CALL
 * Channel</H3> Constructing a <I>any-any</I> CALL channel for a specific
 * <TT>interface</TT> follows exactly the same pattern as in the <I>1-1</I>
 * case. Of course, it must extend <TT>Any2AnyCallChannel</TT> rather than
 * <TT>One2OneCallChannel</TT>.
 * <P>
 * For example, using the same
 * <A HREF="One2OneCallChannel.html#Foo"><TT>Foo</TT></A> interface as before,
 * we derive:
 * 
 * <PRE>
 * import jcsp.lang.*;
 * 
 * public class Any2AnyFooChannel extends Any2AnyCallChannel implements Foo {
 * 
 *   ...  same body as <A HREF=
"One2OneCallChannel.html#One2OneFooChannel"><TT>One2OneFooChannel</TT></A>
 * 
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
 * 
 * class B implements CSProcess, Foo {
 * 
 *   private final ChannelAccept in;
 * 
 *   public B (final One2OneFooChannel in) {         // original constructor
 *     this.in = in;
 *   }
 * 
 *   public B (final Any2AnyFooChannel in) {       // additional constructor
 *     this.in = in;
 *   }
 * 
 *   ...  rest <A HREF="One2OneCallChannel.html#Accept">as before</A>
 * 
 * }
 * </PRE>
 * 
 * When wrapping the above to hide its raw method interface, don't forget to
 * include the extra constructor(s):
 * 
 * <PRE>
 * import jcsp.lang.*;
 * 
 * public class B2 implements CSProcess { // no Foo interface
 * 
 *     private final B b;
 * 
 *     public B2(final One2OneFooChannel in) { // original constructor
 *         b = new B(in);
 *     }
 * 
 *     public B2(final Any2AnyFooChannel in) { // additional constructor
 *         b = new B(in);
 *     }
 * 
 *     public void run() {
 *         b.run();
 *     }
 * 
 * }
 * </PRE>
 *
 * <H3><A NAME="ALTing">ALTing on a CALL Channel</H3> As for <I>ordinary</I>
 * channels, ALTing over <I>1-any</I> or <I>any-any</I> versions is not
 * supported. Hence, a server can only choose to {@link #accept <TT>accept</TT>}
 * or not to <TT>accept</TT> a <TT>Any2AnyFooChannel</TT> - it cannot back off
 * because of some other event.
 *
 * <H3><A NAME="Network">Building a CALL Channel Network</H3> Network building
 * with CALL channels is the same as building with <I>ordinary</I> channels.
 * First construct the channels and, then, construct the processes - plugging in
 * the channels as required and running them in {@link Parallel}.
 * <P>
 * For example, the network consisting of several <I>clients</I> and several
 * <I>servers</I>:
 * <p>
 * <IMG SRC="doc-files\Any2AnyCallChannel1.gif">
 * </p>
 * where <TT>A</TT> is unchanged from its definition in
 * <A HREF="One2OneCallChannel.html#Call"><TT>One2OneCallChannel</TT></A>, is
 * implemented by:
 * 
 * <PRE>
 * Any2AnyFooChannel c = new Any2AnyFooChannel();
 * 
 * final A[] aClients = new A[n_aClients];
 * for (int i = 0; i < aClients.length; i++) {
 *     aClients[i] = new A(c);
 * }
 * 
 * final B2[] bServers = new B2[n_bClients];
 * for (int i = 0; i < bServers.length; i++) {
 *     bServers[i] = new B2(c);
 * }
 * 
 * new Parallel(new CSProcess[] { new Parallel(aClients), new Parallel(bServers) }).run();
 * </PRE>
 * 
 * The <I>clients</I> compete with each other to find a <I>server</I> and the
 * <I>servers</I> compete with each other to find a <I>client</I>. A
 * <I>client</I> must not care which <I>server</I> it gets and vice-versa. The
 * <I>any-any</I> CALL channel strictly serialises <I>client-server</I>
 * transactions - only one at a time can actually take place.
 * <A NAME="good-idea">
 * <P>
 * It is a good idea, therefore, to restrict the duration of the CALL to as
 * short a period as possible, so that other <I>clients</I> and <I>servers</I>
 * are not unnecessarilly delayed. For instance, the <I>any-any</I> CALL can be
 * used just to enable a <I>client</I> and <I>server</I> find one another.
 * During the CALL, the <I>client</I> could give the <I>server</I> a private
 * direct line (e.g. a <I>1-1</I> CALL channel) on which it promises to call
 * back after hanging up on the shared one. The <I>server</I> could reply with
 * special information (such as how long it is prepared to wait for that call
 * back). In this way, multiple <I>client-server</I> transactions could proceed
 * concurrently. An example is given below.
 *
 * <H2><A NAME="Example">Example</H2> This further develops
 * <A HREF="Any2OneCallChannel.html#Example">the CALL channel version</A> of the
 * <I>Wot-no-Chickens</I> example, introduced in <TT>Any2OneCallChannel</TT>.
 * <P>
 * The college authorities have decided to charge its philosophers for all the
 * chickens they are consuming. Unfortunately, getting the canteen to collect
 * money from the customers would mean that its <TT>serviceTime</TT> would have
 * to be increased from its present zero to one second. Since the canteen can
 * only do one thing at a time, a new form of delay would be introduced into the
 * system. Since the philosophers only think for a maximum of three seconds in
 * between trying to eat, having them wait around (possibly for several seconds)
 * while their colleagues make their purchases is deemed unacceptable.
 * <P>
 * The college has decided, therefore, to make some new investments. Students
 * are to be employed to sell the chickens from the canteen. Each student is
 * equipped with a hot-plate on which can be (indefinitely) stored one hot
 * chicken, straight from the canteen. The student can get a chicken from the
 * canteen (if the canteen has some available) still with the same zero
 * <TT>serviceTime</TT> as before - the canteen has not changed. Students that
 * have chickens compete with each other to service any customers. Servicing a
 * customer means collecting some money and giving change, which takes one
 * second, as well as handing over the chicken.
 * <P>
 * Hungry philosophers compete with each other to find any student who is ready
 * for a customer. Students and philosophers find each other through a
 * <I>any-any</I> CALL channel. However, if the full transaction (which includes
 * a delay of one second) were to take place over that shared channel, the
 * college would be no better off! The canteen-students system would still only
 * be dealing with one customer at a time.
 * <P>
 * Therefore, the <A HREF="#good-idea"><I>good idea</I></A> mentioned at the end
 * of the previous section is employed to enable student-philosopher
 * transactions to proceed in parallel. The <I>any-any</I> shared channel is
 * used only for a philosopher to pass over a private direct <I>1-1</I> line to
 * a student and to exchange names (out of politeness). The <I>bulk</I> of all
 * transactions takes place in parallel over unshared lines.
 * <P>
 * Here is the fixed part of the network diagram for the new college. The
 * <I>1-1</I> channels for private communication between a philosopher
 * (<TT>Prof</TT>) and a student (<TT>Student</TT>) are dynamically connected,
 * continually changing and, therefore, not shown:
 * <p>
 * <IMG SRC="doc-files\Any2AnyCallChannel2.gif">
 * </p>
 * For this new system, the
 * <A HREF="Any2OneCallChannel.html#Canteen"><TT>Canteen</TT></A>,
 * <A HREF="Any2OneCallChannel.html#Chef"><TT>Chef</TT></A> and
 * <A HREF="Any2OneCallChannel.html#Clock"><TT>Clock</TT></A> processes are
 * unchanged. The philosophers' behaviour has changed as described above and,
 * so, we have chosen a new name for their class - <TT>Prof</TT>. But first, we
 * describe the new player on the block - the <TT>Student</TT>.
 *
 * <H3><A NAME="Student">The Student</H3> Each student process is a
 * <I>server</I> on its <TT>studentService</TT> channel and a <I>client</I> on
 * its <TT>canteenService</TT>. As with the
 * <A HREF="Any2OneCallChannel.html#Canteen"><TT>Canteen</TT></A> <I>server</I>,
 * we have chosen to define the <TT>Student</TT> service interface and
 * associated CALL channel class as inner declarations:
 * 
 * <PRE>
 * import jcsp.lang.*;
 * 
 * class Student implements CSProcess {
 * 
 *   public static interface Service {
 *     public String hello (String profId,
 *            Canteen.One2OneServiceChannel service);
 *   }
 * 
 *   public static class Any2AnyServiceChannel
 *    extends Any2AnyCallChannel implements Service {
 *     public String hello (String profId,
 *            Canteen.One2OneServiceChannel service) {
 *       join ();
 *       String n = ((Service) server).hello (profId, service);
 *       fork ();
 *       return n;
 *     }
 *   }
 * </PRE>
 * 
 * Note that the <TT>hello</TT> CALL offers the caller's name and private
 * <I>1-1</I> channel. These will be saved and the student's name returned when
 * the student accepts the CALL. Notice also that that private channel instances
 * the <I>1-1</I> <TT>Service</TT> channel exported by the
 * <A HREF="Any2OneCallChannel.html#Canteen"><TT>Canteen</TT></A>. This is not
 * surprising since the student, who will accept the CALL on that line, will be
 * acting on behalf of the canteen - one of many proxies set up by the college
 * to reduce the service bottleneck caused by the newly introduced charges.
 * <P>
 * Following the same pattern as for the <TT>Canteen</TT>, we next set up the
 * constructor and the local fields for saving its parameters:
 * 
 * <PRE>
 * private final String id;
 * private final ChannelAccept studentService;
 * // provide service on this line
 * private final Canteen.Service canteenService;
 * // act as a client on this line
 * private final int serviceTime;
 * 
 * public Student(String id, Any2AnyServiceChannel studentService, Canteen.Service canteenService, int serviceTime) {
 *     this.id = id;
 *     this.studentService = studentService;
 *     this.canteenService = canteenService;
 *     this.serviceTime = serviceTime;
 * }
 * </PRE>
 * 
 * Now, we need to combine the exported interfaces into a single one so that the
 * inner process can be created (anonymously) by this wrapper's run method:
 * 
 * <PRE>
 *   private interface inner extends CSProcess, Service, Canteen.Service {};
 * 
 *   public void run () {
 * 
 *     new inner () {
 * 
 *       private AltingChannelAccept service;
 *       // place holder for the private line
 *       private int nServed = 0;
 * 
 *       private final CSTimer tim = new CSTimer ();
 * </PRE>
 * 
 * Impementations of the required CALL interfaces (including the one for the
 * channel privately supplied) come next:
 * 
 * <PRE>
 *       public String hello (String profId,
 *              Canteen.One2OneServiceChannel service) {
 *         this.service = service;
 *         System.out.println ("   Student " + id + " -> Prof " +
 *                             profId + " : hello ... ");
 *         return id;
 *       }
 * 
 *       public int takeChicken (String profId) {
 *         System.out.println ("   Student " + id + " -> Prof +"
 *                             profId + " : that'll be 1-95 please ... ");
 *         tim.sleep (serviceTime);          // take the money and give any change
 *         nServed++;
 *         System.out.println ("   Student " + id + " -> Prof "
 *                             + profId + " : thank you - have a nice day ["
 *                             + nServed + " served]");
 *         return 1;
 *       }
 * </PRE>
 * 
 * and the run method that conducts everything:
 * 
 * <PRE>
 * public void run() {
 *     System.out.println("   Student " + id + " : starting ... ");
 *     while (true) {
 *         canteenService.takeChicken(id);
 *         // make a client CALL on the canteen
 *         System.out.println("   Student " + id + " : got chicken ... ready to serve ...");
 *         studentService.accept(this); // customer makes contact
 *         service.accept(this); // deal with customer
 *     }
 * }
 * </PRE>
 * 
 * Finally, don't forget to run this inner process:
 * 
 * <PRE>
 *     }.run ();
 *   }
 * }
 * </PRE>
 *
 * <H3><A NAME="Professor">The Professor</H3> Professors behave in pretty much
 * the same way as
 * <A HREF="Any2OneCallChannel.html#Philosopher">philosophers</A>. The only
 * difference is that they communicate with the <A HREF="#Student">student</A>
 * proxies, rather than directly with the
 * <A HREF="Any2OneCallChannel.html#Canteen">canteen</A>. For this, each needs
 * to keep a private channel up its sleeve:
 * 
 * <PRE>
 * import jcsp.lang.*;
 * 
 * class Prof implements CSProcess {
 * 
 *     private final String id;
 *     private final Student.Service studentService;
 *     private final int thinkTime;
 *     private final int eatTime;
 *     private final boolean greedy;
 * 
 *     public Prof(String id, Student.Service studentService, int thinkTime, int eatTime, boolean greedy) {
 *         this.id = id;
 *         this.studentService = studentService;
 *         this.thinkTime = thinkTime;
 *         this.eatTime = eatTime;
 *         this.greedy = greedy;
 *     }
 * 
 *     public void run() {
 *         final CSTimer tim = new CSTimer();
 *         final Canteen.One2OneServiceChannel service = new Canteen.One2OneServiceChannel();
 *         // This is the private channel for
 *         // direct communication with a student
 *         int nEaten = 0;
 *         while (true) {
 *             if (!greedy) {
 *                 System.out.println("   Prof " + id + " : thinking ...");
 *                 tim.sleep(thinkTime); // thinking
 *             }
 *             System.out.println("   Prof " + id + " : gotta eat ...");
 *             String student = studentService.hello(id, service);
 *             // find a student
 *             System.out.println("   Prof " + id + " -> Student " + student + " : hi, can I have a chicken please?");
 *             int chicken = service.takeChicken(id);
 *             // and pay up ...
 *             nEaten++;
 *             System.out.println("   Prof " + id + " : mmm ... that's good [" + nEaten + " so far]");
 *             tim.sleep(eatTime); // eating
 *         }
 *     }
 * 
 * }
 * </PRE>
 *
 * <H3><A NAME="College">The New College</H3> The new charges slow down the
 * speed with which the professors can get their chickens (although not their
 * eating of them). Most affected are the <I>greedy</I> ones who can no longer
 * consume at a rate approaching 10 per second (whilst supplies hold out) but
 * now have to wait a whole second simply to make a purchase. <I>Bill</I>,
 * therefore, should not get quite so sick. Notice that, so long as supplies
 * hold out and there are enough students, philosophers are not delayed by each
 * other - their purchases will be made in parallel.
 * <P>
 * For convenience, the college network diagram is reproduced here - naming some
 * more of the characters. The new college seems to have attracted some good
 * students:
 * <p>
 * <IMG SRC="doc-files\Any2AnyCallChannel3.gif">
 * </p>
 * Here is the code:
 * 
 * <PRE>
 * import jcsp.lang.*;
 * 
 * class NewCollege implements CSProcess {
 * 
 *     public void run() {
 * 
 *         final String[] profId = { "Bill", "Hilary", "Gennifer", "Paula", "Monica" };
 *         final String[] studentId = { "Occam", "Babbage", "Einstein", "Turing" };
 * 
 *         final int thinkTime = 3000; // 3 seconds
 *         final int eatTime = 100; // 100 milliseconds
 * 
 *         final int studentServiceTime = 1000; // 1 second
 * 
 *         final int canteenServiceTime = 0; // 0 seconds
 *         final int canteenSupplyTime = 3000; // 3 seconds
 *         final int maxChickens = 50;
 * 
 *         final Student.Any2AnyServiceChannel studentService = new Student.Any2AnyServiceChannel();
 *         final Canteen.Any2OneServiceChannel canteenService = new Canteen.Any2OneServiceChannel();
 *         final Canteen.Any2OneSupplyChannel supply = new Canteen.Any2OneSupplyChannel();
 * 
 *         final Student[] students = new Student[studentId.length];
 *         for (int i = 0; i < students.length; i++) {
 *             students[i] = new Student(studentId[i], studentService, canteenService, studentServiceTime);
 *         }
 * 
 *         final Prof[] profs = new Prof[profId.length];
 *         for (int i = 0; i < profs.length; i++) {
 *             profs[i] = new Prof(profId[i], studentService, thinkTime, eatTime, i == 0);
 *         }
 * 
 *         new Parallel(new CSProcess[] { new Clock(),
 *                                        new Canteen(canteenService, supply, canteenServiceTime, canteenSupplyTime,
 *                                                    maxChickens),
 *                                        new Parallel(students), new Parallel(profs),
 *                                        new Chef("Pierre", 4, 2000, supply),
 *                                        // chefId, batchSize, batchTime
 *                                        new Chef("Henri", 10, 20000, supply), new Chef("Sid", 100, 150000, supply) })
 *                                                                                                                     .run();
 * 
 *     }
 * 
 *     public static void main(String argv[]) {
 *         new NewCollege().run();
 *     }
 * }
 * </PRE>
 *
 * @see One2OneCallChannel
 * @see jcsp.lang.Any2OneCallChannel
 * @see jcsp.lang.One2AnyCallChannel
 * @see Alternative
 *
 * @author P.H. Welch
 */

public abstract class Any2AnyCallChannel implements ChannelAccept, Serializable {
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
    protected CSProcess server;

    /**
     * This may be set during the standard
     * <A HREF="One2OneCallChannel.html#One2OneFooChannel"> calling sequence</A> to
     * record which method was invoked by a <I>client</I>. It is only safe to do
     * this between the {@link #join <TT>join</TT>} and {@link #fork <TT>fork</TT>}
     * elements of that sequence. Either <I>all</I> the CALL channel methods should
     * do this or <I>none</I> - in the latter case, its default value remains as
     * zero. Its value is returned to a <I>server</I> as the result the
     * <I>server</I>'s invocation of {@link #accept <TT>accept</TT>}.
     */
    protected int selected = 0;

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
    public synchronized int accept(CSProcess server) {
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
        d.write(null);
    }
}
