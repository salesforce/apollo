
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
 * This is the <i>front-end</i> for a <i>barrier</i> that can be used as a
 * {@link Guard} in an {@link Alternative}.
 * <p>
 * <H2>Description</H2> An <i>alting</i> barrier is represented by a family of
 * <code>AltingBarrier</code> <i>front-ends</i>. Each <i>process</i> using the
 * barrier must do so via its own <i>front-end</i>. A new <i>alting</i> barrier
 * is created by the static {@link #create create} method, which returns an
 * array of <i>front-ends</i>. If new processes need to be enrolled, further
 * <i>front-ends</i> may be made from an existing one (see {@link #expand(int)
 * expand} and {@link #contract(AltingBarrier[]) contract}). A process may
 * temporarilly {@link #resign resign} from a barrier and, later,
 * re-{@link #enroll enroll}.
 * <p>
 * To use this barrier, a process simply includes its given
 * <code>AltingBarrier</code> <i>front-end</i> in a {@link Guard} array
 * associated with an {@link Alternative}. Its index will be selected if and
 * only if all parties (processes) to the barrier similarly select it (using
 * their own <i>front-ends</i>).
 * </p>
 * <p>
 * If a process wishes to commit to this barrier (i.e. not offer it as a choice
 * in an {@link Alternative}), it may {@link #sync sync} on it. However, if all
 * parties only do this, a <i>non-alting</i> {@link Barrier} would be more
 * efficient. A further shortcut (over using an {@link Alternative}) is provided
 * to {@link #poll(long) poll} this barrier for completion.
 * </p>
 * <p>
 * An <code>AltingBarrier</code> <i>front-end</i> may only be used by one
 * process at a time (and this is checked at run-time). A process may
 * communicate a non-resigned <i>front-end</i> to another process; but the
 * receiving process must {@link #mark mark} it before using it and, of course,
 * the sending process must not continue to use it. If a process terminates
 * holding a <i>front-end</i>, it may be recycled for use by another process via
 * {@link #reset reset}.
 * </p>
 * <H3>Priorities</H3> These do not <i>-- and cannot --</i> apply to selection
 * between barriers. The {@link Alternative#priSelect priSelect()} method works
 * locally for the process making the offer. If this were allowed, one process
 * might offer barrier <code>x</code> with higher priority than barrier
 * <code>y</code> ... and another process might offer them with its priorities
 * the other way around. In which case, it would be impossible to resolve a
 * choice in favour of <code>x</code> or <code>y</code> in any way that
 * satisfied the conflicting priorities of both processes.
 * </p>
 * <p>
 * However, the {@link Alternative#priSelect priSelect()} method <i>is</i>
 * allowed for choices including barrier guards. It honours the respective
 * priorities defined between <i>non-barrier</i> guards ... and those between a
 * <i>barrier</i> guard and <i>non-barrier</i> guards (which guarantees, for
 * example, immediate response to a timeout from ever-active barriers). Relative
 * priorities between <i>barrier</i> guards are inoperative.
 * </p>
 * <H3>Misuse</H3> The implementation defends against misuse, throwing an
 * {@link AltingBarrierError} error when riled. See the documentation for
 * {@link AltingBarrierError} for circumstances.
 * </p>
 * <H2>Example 0 <i>(a single alting barrier)</i></H2> Here is a simple gadget
 * with two modes of operation, switched by a <i>click</i> event (operated
 * externally by a button in the application below). Initially, it is in
 * <i>individual</i> mode -- represented here by incrementing a number and
 * outputting it (as a <code>String</code> to change the label on its
 * controlling button) as often as it can. Its other mode is <i>group</i>, in
 * which it can only work if all associated gadgets are also in this mode. Group
 * work consists of a single decrement and output of the number (to its button's
 * label). It performs group work as often as the group will allow (i.e. until
 * it, or one of its partner gadgets, is clicked back to <i>individual</i>
 * mode).
 * 
 * <PRE>
 * import jcsp.lang.*;
 * 
 * public class AltingBarrierGadget0 implements CSProcess {
 * 
 *   private final AltingChannelInput click;
 *   private final AltingBarrier group;
 *   private final ChannelOutput configure;
 * 
 *   public AltingBarrierGadget0 (
 *     AltingChannelInput click, AltingBarrier group, ChannelOutput configure
 *   ) {
 *     this.click = click;
 *     this.group = group;
 *     this.configure = configure;
 *   }
 * 
 *   public void run () {
 * 
 *     final Alternative clickGroup =
 *       new Alternative (new Guard[] {click, group});
 * 
 *     final int CLICK = 0, GROUP = 1;
 * 
 *     int n = 0;
 *     configure.write (String.valueOf (n));
 * 
 *     while (true) {
 * 
 *       configure.write (Color.green)                <i>// pretty</i>
 * 
 *       while (!click.pending ()) {                  <i>// individual work mode</i>
 *         n++;                                       <i>// work on our own</i>
 *         configure.write (String.valueOf (n));      <i>// work on our own</i>
 *       }
 *       click.read ();                               <i>// must consume the click</i>
 * 
 *       configure.write (Color.red);                 <i>// pretty</i>
 *       
 *       boolean group = true;                        <i>// group work mode</i>
 *       while (group) {
 *         switch (clickGroup.priSelect ()) {         <i>// offer to work with the group</i>
 *           case CLICK:
 *             click.read ();                         <i>// must consume the click</i>
 *             group = false;                         <i>// back to individual work mode</i>
 *           break;
 *           case GROUP:
 *             n--;                                   <i>// work with the group</i>
 *             configure.write (String.valueOf (n));  <i>// work with the group</i>
 *           break;
 *         }
 *       }
 *       
 *     }
 * 
 *   }
 * 
 * }
 * </PRE>
 * 
 * Here is code for a system of buttons and gadgets, synchronised by an
 * <i>alting barrier</i>. Note that this <i>single</i> event needs an array of
 * <code>AltingBarrier</code> <i>front-ends</i> to operate -- one for each
 * gadget:
 * 
 * <PRE>
 * import jcsp.lang.*;
 * import jcsp.plugNplay.*;
 * 
 * public class AltingBarrierGadget0Demo0 {
 * 
 *   public static void main (String[] argv) {
 * 
 *     final int nUnits = 8;
 *
 *     <i>// make the buttons</i>
 * 
 *     final One2OneChannel[] event = Channel.one2oneArray (nUnits);
 *     
 *     final One2OneChannel[] configure = Channel.one2oneArray (nUnits);
 * 
 *     final boolean horizontal = true;
 * 
 *     final FramedButtonArray buttons =
 *       new FramedButtonArray (
 *         "AltingBarrier: Gadget 0, Demo 0", nUnits, 120, nUnits*100,
 *          horizontal, configure, event
 *       );
 * 
 *     <i>// construct an array of front-ends to a single alting barrier</i>
 * 
 *     final AltingBarrier[] group = AltingBarrier.create (nUnits);
 * 
 *     <i>// make the gadgets</i>
 * 
 *     final AltingBarrierGadget0[] gadgets = new AltingBarrierGadget0[nUnits]; 
 *     for (int i = 0; i < gadgets.length; i++) {
 *       gadgets[i] = new AltingBarrierGadget0 (event[i], group[i], configure[i]);
 *     }
 * 
 *     <i>// run everything</i>
 * 
 *     new Parallel (
 *       new CSProcess[] {
 *         buttons, new Parallel (gadgets)
 *       }
 *     ).run ();
 * 
 *   }
 * 
 * }
 * </PRE>
 * 
 * The very simple <i>"group"</i> work in the above example consists of actions
 * performed independently by each gadget (decrementing the number on its
 * button's label). The (alting) barrier synchronisation ensures that these
 * decrements keep in step with each other.
 * <p>
 * A more interesting gadget would work with other gadgets for <i>group</i> work
 * that really did require them all to be engaged. For example, they resume
 * operation of a machine that would be dangerous if some gadgets (perhaps those
 * responsible for safety aspects) were doing their <i>individual</i> work.
 * </p>
 * <H2>Example 1 <i>(lots of alting barriers)</i></H2> This example derives from
 * a pathological challenge to the management of choice between multiway
 * synchronisations raised by <i>Michael Goldsmith (Formal Systems Europe)</i>.
 * There are three processes (<code>P</code>, <code>Q</code> and <code>R</code>)
 * and theee events (<code>a</code>, <code>b</code> and <code>c</code>).
 * <code>P</code> offers events <code>a</code> and <code>b</code>;
 * <code>Q</code> offers events <code>b</code> and <code>c</code>; and
 * <code>R</code> offers events <code>c</code> and <code>a</code>. If
 * <code>P</code> and <code>Q</code> synchronise on <code>b</code>, they do
 * something (possibly together) then start again. Similarly if <code>Q</code>
 * and <code>R</code> synchronise on <code>c</code> or if <code>R</code> and
 * <code>P</code> synchronise on <code>a</code>. In CSP, the expression is
 * trivial:
 * 
 * <PRE>
 *  P = ((a -> P0); P) [] ((b -> P1); P), and where c is not in the alphabet of P
 *  Q = ((b -> Q0); Q) [] ((c -> Q1); Q), and where a is not in the alphabet of Q
 *  R = ((c -> R0); R) [] ((a -> R1); R), and where b is not in the alphabet of R
 *
 *  SYSTEM = (P || Q || R) \ {a, b, c}
 * </PRE>
 * 
 * To impact their environment (and avoid <i>divergence</i>), the sub-processes
 * <code>P0</code>, <code>P1</code>, <code>Q0</code>, <code>Q1</code>,
 * <code>R0</code> and <code>R1</code> will engage in external events (i.e. not
 * just <code>a</code>, <code>b</code> or <code>c</code>). Additionally,
 * <code>P1</code> and <code>Q0</code> (triggered by <code>b</code>) may engage
 * in other hidden events, not given in the above. The same for <code>Q1</code>
 * and <code>R0</code> (triggered by <code>c</code>) and for <code>R1</code> and
 * <code>P0</code> (triggered by <code>a</code>).
 * </p>
 * <p>
 * In our version, there are <code>N</code> processes and events arranged
 * (logically) around a circle. Each process is either <i>off</i> or on
 * <i>standby</i>, switching between these states on random timeouts. Each
 * process is also attached to a personal button that it uses to indicate its
 * state. When <i>off</i>, it colours its button black; when on <i>standby</i>,
 * light gray.
 * </p>
 * <p>
 * When on <i>standby</i>, each process offers <code>(span+1)</code> events: a
 * timeout, the event with it on the circle and the next <code>(span-1)</code>
 * events going (say) clockwise. It the timeout occurs, it switches to its
 * <i>off</i> state. If one of the events (<code>AltingBarrier</code>) occurs,
 * it must have occured for a consecutive block of <code>span</code> processes
 * (including this one ... somewhere) around the circle. This group now go into
 * a <i>playing</i> state.
 * </p>
 * <p>
 * Not mentioned before is a <i>rail track</i>, made of channels, running round
 * the circle. When playing, the process furthest uptrack choses a colour and
 * sends this to its partners down the track (to which the mulitway
 * synchronisation ensures this group has exclusive access). Each process in the
 * playing group then flashes its button with that colour a fixed (parametrised)
 * number of times. The rate of flashing is coordinated by the
 * <code>AltingBarrier</code> multiway synchronisation event common to the group
 * -- the furthest uptrack process only keeping time for this. After playing,
 * the process switches to its <i>off</i> state.
 * </p>
 * <p>
 * [Note: the above <code>SYSTEM</code> has <code>N</code> equal to
 * <code>3</code>, <code>span</code> equal to <code>2</code>, no <i>off</i>
 * state and no timeout on <i>standby</i>. The channels used to flash the
 * buttons are the external events mentioned and the <i>rail track</i> channels
 * are the hidden extras.]
 * </p>
 * <p>
 * Here is the code for these processes. As usual, the constructor just saves
 * all parameters:
 * 
 * <PRE>
 * import jcsp.lang.*;
 * import jcsp.awt.*;
 * 
 * import java.awt.Color;
 * import java.util.Random;
 * 
 * public class AltingBarrierGadget1 implements CSProcess {
 * 
 *   private final AltingBarrier[] barrier;
 *   private final AltingChannelInput in, click;
 *   private final ChannelOutput out, configure;
 *   private final Color offColour, standbyColour;
 *   private final int offInterval, standbyInterval;
 *   private final int playInterval, flashInterval;
 * 
 *   public AltingBarrierGadget1 (
 *     AltingBarrier[] barrier,
 *     AltingChannelInput in, ChannelOutput out,
 *     AltingChannelInput click, ChannelOutput configure,
 *     Color offColour, Color standbyColour,
 *     int offInterval, int standbyInterval,
 *     int playInterval, int flashInterval
 *   ) {
 *     this.barrier = barrier;
 *     this.in = in;  this.out = out;
 *     this.click = click;  this.configure = configure;
 *     this.offColour = offColour;  this.standbyColour = standbyColour;
 *     this.offInterval = offInterval;  this.standbyInterval = standbyInterval;
 *     this.playInterval = playInterval;  this.flashInterval = flashInterval;
 *   }
 * </PRE>
 * 
 * The <code>barrier</code> array gives this <i>gadget</i> access to the
 * multiway events shared with adjacent siblings. The <code>in</code> and
 * <code>out</code> channel ends are part of the <i>rail track</i> this gadget
 * uses later (when <i>playing</i>). The <code>click</code> and
 * <code>configure</code> channels attach this gadget to its button. The
 * <code>click</code> channel is never used by this gadget -- it's included for
 * completeness should anyone wish to enhance its behaviour. The other
 * parameters are just data.
 * </p>
 * <p>
 * The <code>run()</code> method controls switching between <i>off</i>,
 * <i>standby</i> and <i>playing</i> states. The latter is the choice between
 * all the multiway syncs (and the timeout). It is handled by a <i>fair
 * select</i> on the {@link Alternative}, constructed just once (before loop
 * entry):
 * 
 * <PRE>
 *   public void run () {
 *  
 *     CSTimer tim = new CSTimer ();
 * 
 *     final Random random = new Random ();
 * 
 *     final Guard[] standbyGuard = new Guard[barrier.length + 1];
 *     for (int i = 0; i < barrier.length; i++) {
 *       standbyGuard[i] = barrier[i];
 *     }
 *     standbyGuard[barrier.length] = tim;
 *     final int TIMEOUT = barrier.length;
 *     Alternative standbyAlt = new Alternative (standbyGuard);
 * 
 *     configure.write (Boolean.FALSE);               <i>// disable mouse clicks</i>
 *                                                    <i>// (not used by this gadget)</i>
 *     while (true) {
 * 
 *       configure.write (offColour);
 *       tim.sleep (random.nextInt (offInterval));
 * 
 *       configure.write (standbyColour);
 *       tim.setAlarm (tim.read () + random.nextInt (standbyInterval));
 * 
 *       int choice = standbyAlt.fairSelect ();       <i>// magic synchronisation</i>
 * 
 *       if (choice != TIMEOUT) {
 *         play (choice, random, tim);
 *       }
 *       
 *     }
 *  
 *   }
 * </PRE>
 * 
 * Here is the <i>playing</i> code. Initially, a colour is chosen and passed
 * down the playing group's section of <i>rail track</i>, to which it has
 * exclusive access. The flashing group is coordinated through the group's
 * common event, with just one of them keeping time.
 * 
 * <PRE>
 *   private void play (int choice, Random random, CSTimer tim) {
 *     
 *     final boolean RIGHTMOST = (choice == 0);
 *     final boolean LEFTMOST = (choice == (barrier.length - 1));
 * 
 *     Color colour = null;
 *     if (RIGHTMOST) {
 *       colour = new Color (random.nextInt ());
 *     } else {
 *       colour = (Color) in.read ();
 *     }
 *     Color bright = colour.brighter ();
 * 
 *     if (!LEFTMOST) out.write (colour);             <i>// pass it on</i>
 * 
 *     final AltingBarrier focus = barrier[choice];
 * 
 *     final int count = playInterval/flashInterval;
 * 
 *     long timeout = tim.read () + flashInterval;
 *     
 *     boolean bright = true;
 * 
 *     for (int i = 0; i < count; i++) {
 *       configure.write (bright ? brighter : colour);
 *       bright = !bright;
 *       if (RIGHTMOST) {
 *         tim.after (timeout);
 *         timeout += flashInterval;
 *       }
 *       focus.sync ();
 *     }
 * 
 *   }
 * 
 * }
 * </PRE>
 * 
 * Here is code setting up a <i>"circle"</i> of these gadgets, buttons and
 * alting barriers. The buttons are laid out in a row, so that the rightmost
 * button is actually on the <i>"left"</i> of the leftmost button. Care needs to
 * be taken to distribute the <code>span</code> <i>front-ends</i> for each
 * <code>AltingBarrier</code> to the correct gadgets -- see the
 * <i>re-arrangement</i> below:
 * 
 * <PRE>
 * import jcsp.lang.*;
 * import jcsp.util.*;
 * import jcsp.plugNplay.*;
 * 
 * import java.awt.Color;
 * import java.util.Random;
 * 
 * public class AltingBarrierGadget1Demo0 {
 * 
 *   public static void main (String[] argv) {
 * 
 *     final int nUnits = 30, span = 6;
 *     
 *     final int offInterval = 800, standbyInterval = 1000;    <i>// milliseconds</i>
 *     
 *     final int playInterval = 10000, flashInterval = 500;    <i>// milliseconds</i>
 * 
 *     final Color offColour = Color.black, standbyColour = Color.lightGray;
 * 
 *     <i>// make the buttons</i>
 * 
 *     final One2OneChannel[] click =
 *       Channel.one2oneArray (nUnits, new OverWriteOldestBuffer (1));
 * 
 *     final One2OneChannel[] configure = Channel.one2oneArray (nUnits);
 * 
 *     final boolean horizontal = true;
 * 
 *     final FramedButtonArray buttons =
 *       new FramedButtonArray (
 *         "AltingBarrier: Gadget 1, Demo 0", nUnits, 100, nUnits*50,
 *          horizontal, configure, click
 *       );
 * 
 *     <i>// construct nUnits barriers, each with span front-ends ...</i>
 * 
 *     AltingBarrier[][] ab = new AltingBarrier[nUnits][];
 *     for (int i = 0; i < nUnits; i++) {
 *       ab[i] = AltingBarrier.create (span);
 *     }
 * 
 *     <i>// re-arrange front-ends, ready for distribution to processes ...</i>
 * 
 *     AltingBarrier[][]barrier = new AltingBarrier[nUnits][span];
 *     for (int i = 0; i < nUnits; i++) {
 *       for (int j = 0; j < span; j++) {
 *         barrier[i][j] = ab[(i + j) % nUnits][j];
 *       }
 *     }
 * 
 *     <i>// make the track and the gadgets</i>
 * 
 *     One2OneChannel[] track = Channel.one2oneArray (nUnits);
 * 
 *     AltingBarrierGadget1[] gadgets = new AltingBarrierGadget1[nUnits];
 *     for (int i = 0; i < nUnits; i++) {
 *       gadgets[i] =
 *         new AltingBarrierGadget1 (
 *           barrier[i],
 *           track[(i + 1)%nUnits], track[i],
 *           click[i], configure[i],
 *           offColour, standbyColour,
 *           offInterval, standbyInterval,
 *           playInterval, flashInterval
 *         );
 *     }
 * 
 *     <i>// run everything</i>
 * 
 *     new Parallel (
 *       new CSProcess[] {
 *         buttons, new Parallel (gadgets)
 *       }
 *     ).run ();
 * 
 *   }
 * 
 * }
 * </PRE>
 * 
 * For fun, here is another application program for the same gadget. It allows a
 * much larger system to be built, laying out the <i>circle</i> of buttons in a
 * grid, row by row. The rightmost button on each row is to the <i>left</i> of
 * the leftmost button on the next row down. The <i>next row down</i> from the
 * bottom row is the top row. The buttons and its <code>click</code> and
 * <code>configure</code> channels are now two dimensional structures. The
 * barriers, gadgets and rail track are still one dimensional. Only code
 * differences from the above are shown:
 * 
 * <PRE>
 * import jcsp.lang.*;
 * import jcsp.util.*;
 * import jcsp.plugNplay.*;
 * 
 * import java.awt.Color;
 * import java.util.Random;
 * 
 * public class AltingBarrierGadget1Demo1 {
 * 
 *   public static void main (String[] argv) {
 * 
 *     final int width = 30, depth = 20;
 *     final int nUnits = width*depth;
 *     
 *     <i>...  the other system parameters (final ints and Colors)</i>
 *     
 *     final One2OneChannel[][] click = new One2OneChannel[depth][];
 *     for (int i = 0; i < depth; i++) {
 *       click[i] = Channel.one2oneArray (width, new OverWriteOldestBuffer (1));
 *     }
 *     
 *     final One2OneChannel[][] configure = new One2OneChannel[depth][];
 *     for (int i = 0; i < depth; i++) {
 *       configure[i] = Channel.one2oneArray (width);
 *     }
 * 
 *     final FramedButtonGrid buttons =
 *       new FramedButtonGrid (
 *         "AltingBarrier: Gadget 1, Demo 1", depth, width,
 *         20 + (depth*50), width*50, configure, click
 *       );
 * 
 *     <i>...  construct nUnits barriers and the track exactly as before</i>
 * 
 *     AltingBarrierGadget1[] gadgets = new AltingBarrierGadget1[nUnits];
 *     for (int i = 0; i < nUnits; i++) {
 *       gadgets[i] =
 *         new AltingBarrierGadget1 (
 *           barrier[i],
 *           track[(i + 1)%nUnits], track[i],
 *           click[i/width][i%width],
 *           configure[i/width][i%width],
 *           offColour, standbyColour,
 *           offInterval, standbyInterval,
 *           playInterval, flashInterval
 *         );
 *     }
 * 
 *     <i>...  build and run the buttons and gadgets in parallel</i>
 * 
 *   }
 * 
 * }
 * </PRE>
 * 
 * <H3>Other Examples</H3> The <code>alting-barriers</code> directory in
 * <code>jcsp-demos</code> contains other gadgets in a similar vein.
 * 
 * <H4><code>AltingBarrierGadget2</code></H4> These are similar to
 * <code>AltingBarrierGadget1</code>, but sit on a <i>2-way</i> circular
 * railtrack offering to synchronise in the same <code>span</code>-groups. Their
 * difference is the game they play when synchronised: <i>pass-the-parcel</i> up
 * and down their section of track, with the parcel's (rapid) progress indicated
 * by writing on the button labels. These gadgets also enable their buttons when
 * playing and finish their game when any one, or more, of their buttons is
 * clicked -- or they get bored and timeout.
 * 
 * <H4><code>AltingBarrierGadget3</code></H4> Along with their attached buttons,
 * these form a two dimensional structure covering the surface of a
 * <i>torus</i>. The gadgets on the <i>top</i> row are adjacent to the gadgets
 * on the <i>bottom</i> row. The gadgets on the <i>left</i> column are adjacent
 * to the gadgets on the <i>right</i> column. The demonstration program,
 * <code>AltingBarrierGadget3Demo0</code>, asks the user to choose between
 * various shapes and sizes for the synchronisation groups (<i>pluses</i>,
 * <i>crosses</i> and <i>circles</i>) -- but all groups have the same shape and
 * size.
 * </p>
 * <p>
 * Creation and distribution of the barriers is not done by the demonstration
 * program but, more simply, by the gadgets themselves. Each
 * <code>AltingBarrierGadget3</code> belongs to many synchronisation groups, but
 * has <i>lead</i> responsibility for one. It services the input end of a single
 * channel and is given the (shared) output ends of the service channels to the
 * other gadgets in the group it is leading. <i>[Note: the giver of those output
 * ends is the demonstration program.]</i> It creates the alting barrier (and
 * some other things -- see below) for its lead group. Distribution is by
 * <i>I/O-PAR</i> exchange over their service channels as the gadgets
 * initialise. Each gadget sends the things it made to the gadgets in the group
 * it is leading and receives the same from the leaders of its other
 * synchronisation groups. This is the only use they make of these channels.
 * </p>
 * <p>
 * A synchronised group plays a simple counting game until one of its buttons is
 * clicked or the countdown reaches zero. Termination of the game is, and has to
 * be, simultaneous. This is managed by a <i>shared</i> termination flag, safely
 * operated through <i>phased barrier synchronisation</i> (which lets any
 * process in the group set it in an <i>even phase</i>, with all processes
 * acting on it in the <i>odd</i> phases). Shared label and colour variables
 * (for the group's buttons) are operated similarly. The shared variables are
 * distributed (as fields of a shared object) by the leader gadget, along with
 * the group's <i>alting</i> barrier front-ends, during initialisation.
 * </p>
 * <p>
 * The group's <code>AltingBarrier</code> is used to separate the phases.
 * <i>Alting</i> capability on this barrier enables rapid response to <i>any</i>
 * button click on the group to end the game. The lead gadget controls timing:
 * it <i>alts</i> between a countdown timeout, its button click and a cancel
 * message from the rest of the group (should any of their buttons be clicked)
 * -- following any of these with the barrier sync, scheduling the next phase.
 * The other gadgets <i>alt</i> between their button click and the barrier:
 * response to a click being the (timeout) cancel message to the leader then
 * wait for the barrier; response to the barrier being the next phase.
 * </p>
 * <p>
 * The <code>Any2One</code> cancel channel is a mulitplexing relay from the
 * non-lead buttons to the leader gadget. It is constructed by the lead gadget
 * and distributed to its team alongside the shared variables. The cancel
 * channel must be <i>overwriting buffered</i> to avoid deadlock -- the same as
 * the click channels from buttons. The cancel channel must be cleared at the
 * start of each game -- same as the click channels.
 * </p>
 * <p>
 * <A NAME="user-game"><!-- --></A> <b><i>USER GAME</i></b>: run the demo
 * program on a 30x20 grid (expand to full screen), with circle shapes (say), a
 * radius of 3, off and standBy intervals of 1000 (millisecs), play interval of
 * 10000 (millisecs) and a count interfavl of 200 (millisecs). Your challenge is
 * to zap all the coloured shapes away before any of their counts reach zero and
 * the end of the world happens, <i>:)</i>. How long can you survive?!!
 * </p>
 * 
 * <H4><code>AltingBarrierGadget4</code></H4> These are the same as the previous
 * (<code>AltingBarrierGadget3</code>) gadgets, except that they do not assume
 * that all the synchronisation groups to which they belong have the same shape
 * or size. While they do know the size of the group they lead (from the length
 * of the channel of output ends they are given), that is all they know. In
 * particular, they do not know how many items (barriers etc.) to expect from
 * the leaders of their other groups in the opening exchange.
 * </p>
 * <p>
 * This is solved by giving each gadget a global barrier on which their parallel
 * outputting processes synchronise when they finish their distribution. After
 * this synchronisation, <i>all</i> exchanges must have finished and they can
 * tell their gadgets to proceed.
 * </p>
 * <p>
 * The demonstration program for these gadgets just asks for a size for
 * synchronisation groups and allocates (lead) shapes randomly. It could
 * randomise the sizes as well, but the smaller patterns would always emerge
 * dominant in the synchronisations achieved (simply because they require fewer
 * gadgets to be simultaneously on <i>standby</i> -- i.e. offering mode) and the
 * consequent games.
 * </p>
 * <p>
 * <b><i>USER GAME</i></b>: same as <a href="#user-game">before</a>, except with
 * a span (rather than radius) of 3.
 * </p>
 * 
 * <H4><code>AltingBarrierGadget5</code></H4> These are the same as
 * <code>AltingBarrierGadget4</code>, except for the technique used to signal
 * the end of the initial exchange of information amongst the synchronisation
 * groups. They use a global <i>alting</i> barrier on which both the outputting
 * and inputting partners in the exchange offer to synchronise -- the former
 * when finished outputting and the latter as an alternative to inputting. This
 * is, perhaps, more elegant than the <i>conventional</i> barrier and channel
 * used by the <code>AltingBarrierGadget4</code> gadgets and exercises the
 * {@link #expand()} and {@link #contract()} methods.
 * </p>
 * <p>
 * <b><i>USER GAME</i></b>: same as <a href="#user-game">before</a>, except with
 * a span (rather than radius) of 3.
 * </p>
 * 
 * <H4><code>AltingBarrierGadget6</code></H4> The unbuffered service channels
 * used by <code>AltingBarrierGadget3</code>, <code>AltingBarrierGadget4</code>
 * and <code>AltingBarrierGadget5</code> (in the opening <i>I/O-PAR</i> exchange
 * of information amongst each playing group) force the leaders to synchronise
 * with each of their team members. This is no problem for
 * <code>AltingBarrierGadget3</code>, since all groups have the same size and it
 * receives as many messages as it sends; knowing how many messages it is
 * sending, it knows how large its reception arrays should be. This is not the
 * case for <code>AltingBarrierGadget4</code> and
 * <code>AltingBarrierGadget5</code>, which must first receive into
 * <i>collection</i> objects that can expand to any size.
 * </p>
 * <p>
 * <code>AltingBarrierGadget6</code> performs an <i>asynchronous</i> opening
 * exchange of information, using the collection objects directly to buffer the
 * communication messages and no channels. Further, no internal parallelism is
 * needed for this exchange: the gadgets first add all their messages to the
 * collections held by the rest of their team; then, they synchronise on the
 * global barrier (a <i>non-alting</i> one, the same as used by
 * <code>AltingBarrierGadget4</code>s); finally, they extract the information
 * sent by the leaders of the other teams to which they belong. As before, the
 * barrier synchronisation is crucial for the correctness of this exchange,
 * ensuring that all messages are in place before they are gathered.
 * </p>
 * <p>
 * <b><i>USER GAME</i></b>: same as <a href="#user-game">before</a>, except with
 * a span (rather than radius) of 3.
 * </p>
 * <p>
 * 
 *
 * @see Barrier
 * @see Alternative
 *      <p>
 * 
 * @author P.H. Welch
 */
//}}}

public class AltingBarrier extends Guard implements MultiwaySynchronisation {

    /**
     * This references the barrier on which this is enrolled. Also package visible
     * (nulled by the base.contract methods).
     */
    AltingBarrierBase base;

    /** Link to the next <i>front-end</i> (used by {@link AltingBarrierBase}). */
    AltingBarrier next = null;

    /** The process offering this barrier (protected by the base monitor). */
    private Alternative alt = null;

    /** Safety check (protected by the base monitor). */
    private Thread myThread = null;

    /**
     * Safety check (protected by the base monitor). Also package visible (needed by
     * the base.contract(...) method).
     */
    boolean enrolled = true;

    /** Used to support the {@link #sync() <code>sync</code>} method. */
    private Alternative singleAlt = null;

    /** Used to support the {@link #poll() <code>sync</code>} method. */
    private Alternative pollAlt = null;

    /** Used to support the {@link #poll() <code>sync</code>} method. */
    private CSTimer pollTime = null;

    /** Package-only constructor (used by {@link AltingBarrierBase}). */
    AltingBarrier(AltingBarrierBase base, AltingBarrier next) {
        this.base = base;
        this.next = next;
    }

    /**
     * This creates a new <i>alting</i> barrier with an (initial) enrollment count
     * of <code>n</code>. It provides an array of <code>n</code> <i>front-end</i>s
     * to this barrier. It is the invoker's responsibility to install one of these
     * (by constructor or <code>set</code> method) in each process that will be
     * synchronising on the barrier, <i>before</i> firing up those processes.
     * <p>
     * <i>Note:</i> each process must use a different <i>front-end</i> to the
     * barrier. Usually, a process retains an <code>AltingBarrier</code>
     * <i>front-end</i> throughout its lifetime -- however, see {@link #mark mark}.
     * <p>
     *
     * @param n the number of processes enrolled (initially) on this barrier.
     *          <p>
     *
     * @return an array of <code>n</code> <i>front-end</i>s to this barrier.
     *         <p>
     * 
     * @throws IllegalArgumentException if <code>n</code> <= <code>0</code>.
     */
    public static AltingBarrier[] create(int n) {
        if (n <= 0) {
            throw new IllegalArgumentException("\n*** An AltingBarrier must have at least one process enrolled, not "
            + n);
        }
        return new AltingBarrierBase().expand(n);
    }

    /**
     * This creates a new <i>alting</i> barrier with an (initial) enrollment count
     * of <code>1</code>. It provides a single <i>front-end</i> to the barrier, from
     * which others may be generated (see {@link #expand() expand()}) -- usually
     * <i>one-at-a-time</i> to feed processes individually <i>forked</i> (by a
     * {@link ProcessManager}). It is the invoker's responsibility to install each
     * one (by constructor or <code>set</code> method) in the process that will be
     * synchronising on the barrier, <i>before</i> firing up that process. Usually,
     * a process retains an <code>AltingBarrier</code> <i>front-end</i> throughout
     * its lifetime -- however, see {@link #mark mark}.
     * <p>
     * <i>Note:</i> if a known number of processes needing the barrier are to be run
     * (e.g. by a {@link Parallel}), creating the barrier with an array of
     * <i>front-end</i>s using {@link #create(int) create(n)} would be more
     * convenient.
     * <p>
     *
     * @return a single <i>front-end</i> for this barrier.
     */
    public static AltingBarrier create() {
        return new AltingBarrierBase().expand();
    }

    /**
     * This expands the number of processes enrolled in this <i>alting</i> barrier.
     * <p>
     * Use it when an enrolled process is about to go {@link Parallel} itself and
     * some/all of those sub-processes also need to be enrolled. It returns an array
     * new <i>front-end</i>s for this barrier. It is the invoker's responsibility to
     * pass these on to those sub-processes.
     * </p>
     * <p>
     * Note that if there are <code>x</code> sub-processes to be enrolled, this
     * method must be invoked with an argument of <code>(x - 1)</code>. Pass the
     * <i>returned</i> <code>AltingBarrier</code>s to <i>any</i>
     * <code>(x - 1)</code> of those sub-processes. Pass <i>this</i>
     * <code>AltingBarrier</code> to the last one.
     * </p>
     * <p>
     * Before using its given <i>front-end</i> to this barrier, each sub-process
     * must {@link #mark mark} it to take ownership. <i>[Actually, only the
     * sub-process given the original front-end (which may be running in a different
     * thread) really has to do this.]</i>
     * </p>
     * <p>
     * Following termination of the {@link Parallel}, the original process must take
     * back ownership of its original <code>AltingBarrier</code> <i>(loaned to one
     * of the sub-processes, which may have been running on a different thread)</i>
     * by {@link #mark mark}ing it again.
     * </p>
     * <p>
     * Also following termination of the {@link Parallel}, the original process must
     * contract the number of processes enrolled on the barrier. To do this, it must
     * have retained the <i>front-end</i> array returned by this method and pass it
     * to {@link #contract(AltingBarrier[]) contract}.
     * </p>
     * <p>
     *
     * @param n the number of processes to be added to this barrier.
     *          <p>
     *
     * @return an array of new <i>front-end</i>s for this barrier.
     *         <p>
     * 
     * @throws IllegalArgumentException if <code>n</code> <= <code>0</code>.
     *                                  <p>
     * 
     * @throws AltingBarrierError       if currently resigned or not owner of this
     *                                  <i>front-end</i>.
     */
    public AltingBarrier[] expand(int n) {
        if (n <= 0) {
            throw new IllegalArgumentException("\n*** Expanding an AltingBarrier must be by at least one, not " + n);
        }
        synchronized (base) {
            if (myThread == null) {
                myThread = Thread.currentThread();
            } else if (myThread != Thread.currentThread()) {
                throw new AltingBarrierError("\n*** AltingBarrier expand attempted by non-owner.");
            }
            if (!enrolled) {
                throw new AltingBarrierError("\n*** AltingBarrier expand attempted whilst resigned.");
            }
            return base.expand(n);
        }
    }

    /**
     * This expands by one the number of processes enrolled in this <i>alting</i>
     * barrier.
     * <p>
     * Use it when an enrolled process is about to <i>fork</i> a new process (using
     * {@link ProcessManager}) that also needs to be enrolled. It returns an new
     * <i>front-end</i> for this barrier. It is the invoker's responsibility to pass
     * it to the new process.
     * </p>
     * <p>
     * Before terminating, the <i>forked</i> process should {@link #contract()
     * contract} (by one) the number of processes enrolled in this barrier.
     * Otherwise, no further synchronisations on this barrier would be able to
     * complete.
     * </p>
     * <p>
     *
     * @return a new <i>front-end</i> for this barrier.
     *         <p>
     * 
     * @throws AltingBarrierError if currently resigned or not owner of this
     *                            <i>front-end</i>.
     */
    public AltingBarrier expand() {
        synchronized (base) {
            if (myThread == null) {
                myThread = Thread.currentThread();
            } else if (myThread != Thread.currentThread()) {
                throw new AltingBarrierError("\n*** AltingBarrier expand attempted by non-owner.");
            }
            if (!enrolled) {
                throw new AltingBarrierError("\n*** AltingBarrier expand attempted whilst resigned.");
            }
            return base.expand();
        }
    }

    /**
     * This contracts the number of processes enrolled in this <i>alting</i>
     * barrier. The given <i>front-end</i>s are discarded.
     * <p>
     * Use it following termination of a {@link Parallel}, some/all of whose
     * sub-processes were enrolled by being given <i>front-end</i>s returned by
     * {@link #expand(int) expand}. See the documentation for {@link #expand(int)
     * expand}.
     * </p>
     * <p>
     * <i>Warning:</i> only the process that went {@link Parallel} should invoke
     * this method -- never one of the sub-processes.
     * </p>
     * <p>
     * <i>Warning:</i> never invoke this method whilst processes using its
     * argument's <i>front-end</i>s are running.
     * </p>
     * <p>
     * <i>Warning:</i> do not attempt to reuse any of the argument elements
     * afterwards -- they <i>front-end</i> nothing.
     * </p>
     * <p>
     *
     * @param ab the <i>front-ends</i> being discarded from this barrier. This array
     *           must be unaltered from one previously delivered by an
     *           {@link #expand(int) expand}.
     *           <p>
     * 
     * @throws IllegalArgumentException if <code>ab</code> is <code>null</code> or
     *                                  zero length.
     *                                  <p>
     * 
     * @throws AltingBarrierError       if the given array is <i>not</i> one
     *                                  previously delivered by an
     *                                  {@link #expand(int) expand(n)}, or the
     *                                  invoking process is currently resigned or
     *                                  not the owner of this <i>front-end</i>.
     */
    public void contract(AltingBarrier[] ab) {
        if (ab == null) {
            throw new IllegalArgumentException("\n*** AltingBarrier contract given a null array.");
        }
        if (ab.length == 0) {
            throw new IllegalArgumentException("\n*** AltingBarrier contract given an empty array.");
        }
        synchronized (base) {
            // if (myThread == null) { // contract on
            // myThread = Thread.currentThread (); // a virgin AltingBarrier
            // } // is an error ???
            // else // (PHW)
            if (myThread != Thread.currentThread()) {
                throw new AltingBarrierError("\n*** AltingBarrier contract attempted by non-owner.");
            }
            if (!enrolled) {
                throw new AltingBarrierError("\n*** AltingBarrier contract attempted whilst resigned.");
            }
            base.contract(ab);
        }
    }

    /**
     * This contracts by one the number of processes enrolled in this <i>alting</i>
     * barrier. This <i>front-end</i> cannot not be used subsequently.
     * <p>
     * This method should be used on individually created <i>front-end</i>s (see
     * {@link #expand() expand()}) when, and only when, the process holding it is
     * about to terminate. Normally, that process would have been <i>forked</i> by
     * the process creating this barrier.
     * </p>
     * <p>
     * <i>Warning:</i> do not try to use this <i>front-end</i> following invocation
     * of this method -- it no longer fronts anything.
     * </p>
     * <p>
     *
     * @throws AltingBarrierError if currently resigned or not the owner of this
     *                            <i>front-end</i>.
     */
    public void contract() {
        synchronized (base) {
            // if (myThread == null) { // contract on
            // myThread = Thread.currentThread (); // a virgin AltingBarrier
            // } // is an error ???
            // else // (PHW)
            if (myThread != Thread.currentThread()) {
                throw new AltingBarrierError("\n*** AltingBarrier contract attempted by non-owner.");
            }
            if (!enrolled) {
                throw new AltingBarrierError("\n*** AltingBarrier contract attempted whilst resigned.");
            }
            base.contract(this);
        }
    }

    @Override
    boolean enable(Alternative a) { // package-only visible
        synchronized (base) {
            if (myThread == null) {
                myThread = Thread.currentThread();
            } else if (myThread != Thread.currentThread()) {
                throw new AltingBarrierError("\n*** AltingBarrier front-end enable by more than one Thread.");
            }
            if (!enrolled) {
                throw new AltingBarrierError("\n*** AltingBarrier front-end enable whilst resigned.");
            }
            if (alt != null) { // in case the same barrier
                return false; // occurs more than once in
            } // the same Alternative.
            if (base.enable()) {
                a.setBarrierTrigger(); // let Alternative know we did it
                return true;
            } else {
                alt = a;
                return false;
            }
        }
    }

    @Override
    boolean disable() { // package-only visible
        synchronized (base) {
            if (alt == null) { // in case the same barrier
                return false; // occurs more than once in
            } // the same Alternative.
            if (base.disable()) {
                alt.setBarrierTrigger(); // let Alternative know we did it
                alt = null;
                return true;
            } else {
                alt = null;
                return false;
            }
        }
    }

    /**
     * This is the call-back from a successful 'base.enable'. If it was us that
     * invoked 'base.enable', our 'alt' is null and we don't need to be scheduled!
     * If we are resigned, ditto. Whoever is calling this 'schedule' has the 'base'
     * monitor.
     */
    void schedule() { // package-only visible
        if (alt != null) {
            alt.schedule();
        }
    }

    /**
     * A process may resign only if it is enrolled. A resigned process may not offer
     * to synchronise on this barrier (until a subsequent {@link #enroll enroll}).
     * Other processes can complete the barrier (represented by this front-end)
     * without participation by the resigned process.
     * <p>
     * Unless <i>all</i> processes synchronising on this barrier terminate in the
     * same phase, it is usually appropriate for a terminating process to
     * <i>resign</i> first. Otherwise, its sibling processes will never be able to
     * complete another synchronisation.
     * </p>
     * <p>
     * <i>Note:</i> a process must not transfer its <i>front-end</i> to another
     * process whilst resigned from the barrier -- see {@link #mark mark}.
     * </p>
     * <p>
     * 
     * @throws AltingBarrierError if currently resigned.
     */
    public void resign() {
        synchronized (base) {
            if (!enrolled) {
                throw new AltingBarrierError("\n*** AltingBarrier.resign() whilst not enrolled.");
            }
            enrolled = false;
            base.resign();
        }
    }

    /**
     * A process may enroll only if it is resigned. A re-enrolled process may resume
     * offering to synchronise on this barrier (until a subsequent {@link #resign
     * resign}). Other processes cannot complete the barrier (represented by this
     * front-end) without participation by the re-enrolled process.
     * <p>
     * <i>Note:</i> timing re-enrollment on a barrier usually needs some care. If
     * the barrier is being used for synchronising phases of execution between a set
     * of processes, it is crucial that re-enrollment occurs in an appropriate
     * <i>(not arbitrary)</i> phase. If the trigger for re-enrollment comes from
     * another enrolled process, that process should be in such an appropriate
     * phase. The resigned process should re-enroll and, then, acknowledge the
     * trigger. The triggering process should wait for that acknowledgement. If the
     * decision to re-enroll is internal (e.g. following a timeout), a <i>buddy</i>
     * process, enrolled on the barrier, should be asked to provide that trigger
     * when in an appropriate phase. The <i>buddy</i> process, perhaps specially
     * built just for this purpose, polls a service channel for that question when
     * in that phase.
     * 
     * @throws AltingBarrierError if currently enrolled.
     */
    public void enroll() {
        synchronized (base) {
            if (enrolled) {
                throw new AltingBarrierError("\n*** AltingBarrier.enroll() whilst not resigned.");
            }
            enrolled = true;
            base.enroll();
        }
    }

    /**
     * A process may hand its barrier front-end over to another process, but the
     * receiving process must invoke this method before using it. Beware that the
     * process that handed it over must no longer use it.
     * </p>
     * <p>
     * <i>Note:</i> a process must not transfer its <i>front-end</i> to another
     * process whilst resigned from the barrier -- see {@link #resign resign}. The
     * receiving process assumes this is the case. This <i>mark</i> will fail if it
     * is not so.
     * </p>
     * <p>
     * See {@link #expand(int) expand(n)} for an example pattern of use.
     * <p>
     * 
     * @throws AltingBarrierError if the front-end is resigned.
     */
    public void mark() {
        synchronized (base) {
            if (!enrolled) {
                throw new AltingBarrierError("\n*** Attempt to AltingBarrier.mark() a resigned front-end.");
            }
            myThread = Thread.currentThread();
        }
    }

    /**
     * This resets a <i>front-end</i> for reuse. It still fronts the same barrier.
     * Following this method, this front-end is <i>enrolled</i> on the barrier and
     * not <i>owned</i> by any process.
     * </p>
     * <p>
     * <i>Warning:</i> this should only be used to recycle a <i>front-end</i> whose
     * process has terminated. It should not be used to transfer a <i>front-end</i>
     * between running processes (for which {@link #mark mark} should be used).
     * </p>
     * <p>
     * <i>Example</i>:
     * 
     * <pre>
     *   AltingBarrier[] action = AltingBarrier.create (n);
     * 
     *   Parallel[] system = new Parallel[n];
     *   for (int i = 0; i < system.length; i++) {
     *     system[i] = new Something (action[i], ...);
     *   }
     * 
     *   while (true) {
     *     // invariant: all 'action' front-ends are enrolled on the barrier.
     *     // invariant: all 'action' front-ends are not yet <i>owned</i> by any process.
     *     system.run ();
     *     // assume: no 'system' process discards (contracts) its 'action' front-end.
     *     // note: some 'system' processes may have resigned their 'action' front-ends.
     *     // note: in the next run of 'system', its processes may be <i>different</i>
     *     //       from the point of view of the 'action' front-ends.
     *     for (int i = 0; i < action.length; i++) {
     *       action[i].reset ();
     *     }
     *     // deduce: loop invariant re-established.
     *   }
     * </pre>
     */
    public void reset() {
        synchronized (base) {
            if (!enrolled) {
                enrolled = true;
                base.enroll();
            }
            myThread = null;
        }
    }

    /**
     * This is a simple way to perform a <i>committed</i> synchonisation on an
     * {@link AltingBarrier} without having to set up an {@link Alternative}. For
     * example, if <code>group</code> is an <code>AltingBarrier</code>, then:
     * 
     * <PRE>
     * group.sync();
     * </PRE>
     * 
     * saves first having to construct the single guarded:
     * 
     * <PRE>
     * Alternative groupCommit = new Alternative(new Guard[] { group });
     * </PRE>
     * 
     * and then:
     * 
     * <PRE>
     * groupCommit.select();
     * </PRE>
     * 
     * If this is the only method of synchronisation performed by all parties to
     * this barrier, a <i>non-alting</i> {@link Barrier} would be more efficient.
     * </p>
     * <p>
     * <i>Important note:</i> following a <code>select</code>,
     * <code>priSelect</code> or <code>fairSelect</code> on an {@link Alternative}
     * that returns the index of an <code>AltingBarrier</code>, that barrier
     * synchronisation has happened. Do not proceed to invoke this <code>sync</code>
     * method -- unless, of course, you want to wait for a second synchronisation.
     */
    public void sync() {
        if (singleAlt == null) {
            singleAlt = new Alternative(new Guard[] { this });
        }
        singleAlt.priSelect();
    }

    /**
     * This is a simple way to <i>poll</i> for synchonisation on an
     * {@link AltingBarrier} without having to set up an {@link Alternative}. The
     * parameter specifies how long this poll should leave its offer to synchronise
     * on the table. If <code>true</code> is returned, the barrier has completed. If
     * <code>false</code>, the barrier was unable to complete within the time
     * specified (i.e. at no time were <i>all</i> parties making an offer).
     * </p>
     * <p>
     * For example, if <code>group</code> is an <code>AltingBarrier</code>, then:
     * 
     * <PRE>
     *     if (group.poll (offerTime)) {
     *       ...  group synchronisation achieved
     *     } else {
     *       ...  group synchronisation failed (within offerTime millisecs)
     *     }
     * </PRE>
     * 
     * is equivalent to:
     * 
     * <PRE>
     *     groupTimer.setAlarm (groupTimer.read () + offerTime);
     *     if (groupPoll.priSelect () == 0) {
     *       ...  group synchronisation achieved
     *     } else {
     *       ...  group synchronisation failed (within offerTime millisecs)
     *     }
     * </PRE>
     * 
     * where first would have to have been constructed:
     * 
     * <PRE>
     * CSTimer groupTimer = new CSTimer();
     * Alternative groupPoll = new Alternative(new Guard[] { group, groupTimer });
     * </PRE>
     * 
     * <i>Note:</i> polling algorithms should generally be a last resort! If all
     * parties to this barrier only use this method, synchronisation depends on all
     * their poll periods coinciding. An <code>offerTime</code> of zero is allowed:
     * if all other parties are offering, the barrier will complete -- otherwise,
     * the poll returns immediately. However, if more than one party only ever polls
     * like this, no synchronisation will ever take place.
     * </p>
     * <p>
     *
     * @param offerTime the time (in milliseconds) that this offer to synchronise
     *                  should be left on the table.
     *                  <p>
     *
     * @return <code>true</code> if and only if the barrier completes within time
     *         specifed.
     */
    public boolean poll(long offerTime) {
        if (pollAlt == null) {
            pollTime = new CSTimer();
            pollAlt = new Alternative(new Guard[] { this, pollTime });
        }
        pollTime.setAlarm(pollTime.read() + offerTime);
        return (pollAlt.priSelect() == 0);
    }

}
