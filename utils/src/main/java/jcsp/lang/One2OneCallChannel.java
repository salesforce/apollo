
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
 * This is the super-class for one-to-one <TT>interface</TT>-specific CALL
 * channels.
 * <P>
 * <A HREF="#constructor_summary">Shortcut to the Constructor and Method
 * Summaries.</A>
 *
 * <H2>Description</H2> Normal method invocation between objects is sometimes
 * refered to as <I>message-passing</I> between a <I>client</I> (the invoker of
 * the method) object and a <I>server</I> object (the invoked). Information
 * flows from the client (the method name and parameter values) to the server,
 * which reacts in some way (possibly changing state) and may return information
 * back to the client (via the method result or, indirectly, by changing values
 * referenced by the method parameters).
 * <P>
 * This corresponds loosely with the <I>client-server</I> communication pattern
 * between active processes, but where information flows over a pair of channels
 * connecting them. However, there are major semantic differences between the
 * two mechanisms. A server <I>process</I> is in charge of its own life, can act
 * spontaenously and can refuse to accept a client call. A server <I>object</I>
 * is passive, never does anything unless invoked and, then, has no choice but
 * to obey. We must be careful, therefore, not to become confused by the same
 * words computer scientists have chosen to mean very different things.
 * <P>
 * CALL channels risk deepening this confusion. They provide a method interface
 * for <I>client-server</I> communication between active processes, yet their
 * semantics remain those of a synchronising zero-buffered channel. However, the
 * attraction of replacing a sequence of channel <TT>write</TT>(s) and
 * <TT>read</TT> (matched at the other end of the channel-pair by a sequence of
 * channel <TT>read</TT>(s) and <TT>write</TT>) with a <I>single</I> method
 * invocation (and a <I>single</I> matching {@link #accept <TT>accept</TT>})
 * makes this risk worthwhile.
 * <P>
 * [<I>Note:</I> CALL channels were part of the <B>occam3</B> language
 * specification. They provide an <I>extended rendezvous</I> in the sense of an
 * <B>Ada</B> <TT>entry</TT>, but are considerably more flexible and
 * lightweight.]
 *
 * <H3><A NAME="Convert">Converting a Method Interface into a Variant CALL
 * Channel</H3> A CALL channel must be individually constructed to support a
 * specific method <TT>interface</TT>. This is done by extending this class to
 * implement that <TT>interface</TT> in the manner outlined below. The calling
 * (i.e. <I>client</I>) process just sees that <TT>interface</TT> and invokes
 * its methods. The callee (i.e. <I>server</I>) process just sees the
 * {@link #accept <TT>accept</TT>} method of the CALL channel and invokes that
 * (supplying itself as argument). Both actions are voluntary and have to occur
 * for the communication to take place.
 * <P>
 * <A NAME="Foo"> Consider the following:
 * 
 * <PRE>
 * interface Foo {
 * <I></I>
 *   public Bar calculate (...);
 *   public void processQuery (...);
 *   public boolean closeValve (...);
 * <I></I>
 * }
 * </PRE>
 * 
 * <A NAME="One2OneFooChannel"> Deriving the corresponding CALL channel is
 * mechanical and could easilly be automated:
 * 
 * <PRE>
 * import jcsp.lang.*;
 * <I></I>
 * public class One2OneFooChannel extends One2OneCallChannel implements Foo {
 * <I></I>
 *   public static final int CALCULATE = 0;           // optional
 *   public static final int PROCESS_QUERY = 1;       // optional
 *   public static final int CLOSE_VALVE = 2;         // optional
 * <I></I>
 *   public Bar calculate (...) {
 *     join ();                                       // ready to make the CALL
 *     Bar result = ((Foo) server).calculate (...);
 *     selected = CALCULATE;                          // optional
 *     fork ();                                       // call finished
 *     return result;
 *   }
 * <I></I>
 *   public void processQuery (...) {
 *     join ();                                       // ready to make the CALL
 *     ((Foo) server).processQuery (...);
 *     selected = PROCESS_QUERY;                      // optional
 *     fork ();                                       // call finished
 *   }
 * <I></I>
 *   public boolean closeValve (...) {
 *     join ();                                       // ready to make the CALL
 *     boolean result = ((Foo) server).closeValve (...);
 *     selected = CLOSE_VALVE;                        // optional
 *     fork ();                                       // call finished
 *     return result;
 *   }
 * <I></I>
 * }
 * </PRE>
 * 
 * The above methods will be called by the <I>client</I> process. The
 * {@link #join <TT>join</TT>} will not complete until the <I>server</I> invokes
 * an {@link #accept <TT>accept</TT>} on this channel. That <TT>accept</TT> will
 * not complete until the <I>client</I> reaches the {@link #fork <TT>fork</TT>}.
 * This gives the zero-buffered fully synchronised semantics of CSP channel
 * communication.
 * <P>
 * In between the <TT>join</TT> and the <TT>fork</TT>, the <I>client</I> and
 * <I>server</I> processes are locked together in <I>extended rendezvous</I>,
 * commonly executing the chosen method in the <I>server</I> environment.
 * [Actually, it is the <I>client</I> that invokes the method on the
 * <I>server</I>, temporarilly referenced by the {@link #server <TT>server</TT>}
 * field from this <TT>One2OneCallChannel</TT> super-class and blocked waiting
 * for its <TT>accept</TT> to complete.] Setting the {@link #selected
 * <TT>selected</TT>} field is optional. However, its value is returned to the
 * <I>server</I> by the {@link #accept <TT>accept</TT>} method and can be used
 * (as in the above) to let the <I>server</I> know which of its methods the
 * <I>client</I> invoked.
 * <P>
 * [<I>Note:</I> a <TT>One2OneFooChannel</TT> is only safe to use between a
 * <I>single</I> client and a <I>single</I> server. <I>Any-1</I>, <I>1-Any</I>
 * and <I>Any-Any</I> versions may be derived from {@link Any2OneCallChannel},
 * {@link One2AnyCallChannel} and {@link Any2AnyCallChannel} (respectively)
 * using <I>exactly the same</I> pattern as above.]
 *
 * <H3><A NAME="Call">Calling a CALL Channel</H3> All the <I>client</I> needs to
 * see is the method <TT>interface</TT> implemented by the CALL channel. For
 * example:
 * 
 * <PRE>
 * import jcsp.lang.*;
 * <I></I>
 * public class A implements CSProcess {
 * <I></I>
 *   private final Foo out;
 * <I></I>
 *   public A (final Foo out) {
 *     this.out = out;
 *   }
 * <I></I>
 *   public void run () {
 *     ...
 *     Bar t = out.calculate (...);
 *     ...
 *     out.processQuery (...);
 *     ...
 *     if (! out.closeValve (...)) ...;
 *     ...
 *   }
 * <I></I>
 * }
 * </PRE>
 * 
 * [<I>Note:</I> this means, of course, that a <I>client</I> is blind to the
 * variety of CALL channel it is calling. It may be connected to its
 * <I>server(s)</I> via a <I>1-1</I>, <I>Any-1</I>, <I>1-Any</I> or
 * <I>Any-Any</I> link without any change in its coding.]
 *
 * <H3><A NAME="Accept">Accepting a CALL Channel</H3> To receive the calls
 * forwarded by the CALL channel, the <I>server</I> needs to implement the same
 * <TT>interface</TT>. To accept a call, it invokes the {@link #accept
 * <TT>accept</TT>} method of the CALL channel, passing itself (usually
 * <TT>this</TT>) as the argument. All it needs to see, therefore, is the
 * {@link ChannelAccept} interface implemented by the channel. For example:
 * 
 * <PRE>
 * import jcsp.lang.*;
 * <I></I>
 * class B implements CSProcess, Foo {
 * <I></I>
 *   private final ChannelAccept in;
 * <I></I>
 *   public B (final One2OneFooChannel in) {
 *     this.in = in;
 *   }
 * <I></I>
 *   ...  other fields, methods etc.
 * <I></I>
 *   ...  implementation of Foo methods
 * <I></I>
 *   public void run () {        // controls when Foo invocations are acceptable
 *     ...
 *     in.accept (this);                   // don't care which method was called
 *     ...
 *     switch (in.accept (this)) {         // care which method was called
 *       case One2OneFooChannel.CALCULATE:
 *         ...
 *       break;
 *       case One2OneFooChannel.PROCESS_QUERY:
 *         ...
 *       break;
 *       case One2OneFooChannel.CLOSE_VALVE:
 *         ...
 *       break;
 *     }]
 *     ...
 *     in.accept (this);                   // don't care which method was called
 *     ...
 *   }
 * <I></I>
 * }
 * </PRE>
 * 
 * However, it is not very secure for a server <I>process</I> (like <TT>B</TT>)
 * to advertise a standard <I>method interface</I> (like <TT>Foo</TT>). In the
 * above example, there is the danger that someone might try to invoke one of
 * the <TT>Foo</TT> methods <I>directly</I> on an instance of <TT>B</TT> (e.g.
 * by plugging an instance of <TT>B</TT>, instead of the CALL channel, into an
 * instance of <TT>A</TT>). That would not be a good idea!
 * <P>
 * It is also semantically misleading - <TT>B</TT>'s interface is through the
 * CALL channel passed to its constructor, not through its (necessarilly
 * <TT>public</TT>) <TT>Foo</TT> methods.
 * <P>
 * So, <TT>B</TT> should not be the <I>public</I> server process - we need to
 * hide its directly invocable methods. A simple way to do this is to wrap it up
 * in another process that simply omits the public declaration of the relevant
 * interface:
 * 
 * <PRE>
 * import jcsp.lang.*;
 * <I></I>
 * public class B2 implements CSProcess {            // no Foo interface
 * <I></I>
 *   private final B b;
 * <I></I>
 *   public B2 (final One2OneFooChannel in) {
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
 * Notice that this wrapper imposes no run-time overhead, apart from a small
 * start-up cost. The hidden inner process does all the work and has
 * <I>direct</I> access to the CALL channel and, hence, to the <I>client</I>.
 * <P>
 * [<I>Note:</I> the only difference needed in the <I>server</I> code to support
 * <I>Any-1</I>, <I>1-Any</I> and <I>Any-Any</I> CALL channels is in the
 * parameter declaration that specifies the variety to be used. For complete
 * flexibility, constructors (or <TT>setFooChannel</TT> methods) for each kind
 * (i.e.
 * <A HREF="One2OneCallChannel.html#Accept"><TT>One2OneFooChannel</TT></A>,
 * <A HREF="Any2OneCallChannel.html#Accept"><TT>Any2OneFooChannel</TT></A>,
 * <A HREF="One2AnyCallChannel.html#Accept"><TT>One2AnyFooChannel</TT></A> and
 * <A HREF="Any2AnyCallChannel.html#Accept"><TT>Any2AnyFooChannel</TT></A> may
 * be provided.]
 *
 * <H3><A NAME="ALTing">ALTing on a CALL Channel</H3> This class is a sub-class
 * of {@link Guard} and, therefore, its derived CALL channels may be included in
 * a <TT>Guard</TT> array associated with an {@link Alternative}. Hence, the
 * server may ALT between any mixture of CALL channels, <I>ordinary</I>
 * channels, {@link CSTimer timeouts} and {@link Skip Skips}.
 * <P>
 * However, when implementing the server, the CALL channel field needs to be
 * declared as an {@link AltingChannelAccept}, rather than a
 * {@link ChannelAccept}. So, in the <A HREF="#Accept">above example</A>, the
 * first field declaration of <TT>B</TT> needs to become:
 * 
 * <PRE>
 * private final AltingChannelAccept in;
 * </PRE>
 * 
 * See <A HREF="Any2OneCallChannel.html#Example"><TT>Any2OneCallChannel</TT></A>
 * for an example of <TT>ALT</TT>ing between CALL channels.
 * <P>
 * [<I>Note:</I> a <I>server</I> may ALT on a <I>Any-1</I> CALL channel with
 * this same change. However, as for <I>ordinary</I> channels, ALTing over
 * <I>1-Any</I> or <I>Any-Any</I> versions is not supported.]
 *
 * <H3><A NAME="Network">Building a CALL Channel Network</H3> Network building
 * with CALL channels is the same as building with <I>ordinary</I> channels.
 * First construct the channels and, then, construct the processes - plugging in
 * the channels as required and running them in {@link Parallel}.
 * <P>
 * For example, the simple two process network:
 * <p>
 * <IMG SRC="doc-files\One2OneCallChannel1.gif">
 * </p>
 * is implemented by:
 * 
 * <PRE>
 *     One2OneFooChannel c = new One2OneFooChannel ();
 * <I></I>
 *     new Parallel (
 *       new CSProcess[] {
 *         new A (c),
 *         new B2 (c)
 *       }
 *     ).run ();
 * </PRE>
 * <P>
 * [<I>Note:</I> simple network examples using
 * <A HREF="Any2OneCallChannel.html#Network"><I>Any-1</I></A>,
 * <A HREF="One2AnyCallChannel.html#Network"><I>1-Any</I></A> and
 * <A HREF="Any2AnyCallChannel.html#Network"><I>Any-Any</I></A> CALL channels
 * are given in their respective classes.]
 *
 * <H2><A NAME="Example">Example</H2> Please see
 * <A HREF="Any2OneCallChannel.html#Example"><TT>Any2OneCallChannel</TT></A> for
 * an example that includes one <I>server</I>, ALTing between two (<I>Any-1</I>)
 * CALL channels, and lots of <I>clients</I>.
 *
 * @see Any2OneCallChannel
 * @see One2AnyCallChannel
 * @see Any2AnyCallChannel
 * @see Alternative
 *
 * @author P.H. Welch
 */

public abstract class One2OneCallChannel extends AltingChannelAccept implements Serializable {
    private static final long        serialVersionUID = 1L;
    /**
     * This is used to synchronise the calling and accepting process.
     */
    final private One2OneChannelImpl c                = new One2OneChannelImpl();

    /**
     * This holds a reference to a <I>server</I> process so that a <I>client</I> may
     * make the call. The reference is only valid between the {@link #join
     * <TT>join</TT>} and {@link #fork <TT>fork</TT>} elements of the standard
     * <A HREF="#One2OneFooChannel">calling sequence</A>. As shown in that sequence,
     * it will need casting up to the relevant interface supported by the specific
     * CALL channel derived from this class.
     */
    protected CSProcess server;

    /**
     * This may be set during the standard <A HREF="#One2OneFooChannel">calling
     * sequence</A> to record which method was invoked by a <I>client</I>. It is
     * only safe to do this between the {@link #join <TT>join</TT>} and {@link #fork
     * <TT>fork</TT>} elements of that sequence. Either <I>all</I> the CALL channel
     * methods should do this or <I>none</I> - in the latter case, its default value
     * remains as zero. Its value is returned to a <I>server</I> as the result the
     * <I>server</I>'s invocation of {@link #accept <TT>accept</TT>}.
     */
    protected int selected = 0;

    /**
     * This is invoked by a <I>server</I> when it commits to accepting a CALL from a
     * <I>client</I>. The parameter supplied must be a reference to this
     * <I>server</I> - see the <A HREF="#Accept">example above</A>. It will not
     * complete until a CALL has been made. If the derived CALL channel has set the
     * {@link #selected} field in the way defined by the standard
     * <A HREF="#One2OneFooChannel">calling sequence</A>, the value returned by this
     * method will indicate which method was called.
     *
     * @param server the <I>server</I> process receiving the CALL.
     */
    @Override
    public int accept(CSProcess server) {
        this.server = server;
        c.read(); // ready to ACCEPT the CALL
        c.read(); // wait until the CALL is complete
        return selected;
    }

    /**
     * This is invoked by a <I>client</I> during the standard
     * <A HREF="#One2OneFooChannel">calling sequence</A>. It will not complete until
     * a <I>server</I> invokes an {@link #accept <TT>accept</TT>} on this channel.
     * In turn, that <TT>accept</TT> will not complete until the <I>client</I>
     * invokes a {@link #fork <TT>fork</TT>}, after having made its CALL on the
     * <I>server</I>.
     */
    protected void join() {
        c.write(null);
    }

    /**
     * This is invoked by a <I>client</I> during the standard
     * <A HREF="#One2OneFooChannel">calling sequence</A>. A <I>server</I> must have
     * invoked an {@link #accept <TT>accept</TT>} for the <I>client</I> to have got
     * this far in the sequence - see the {@link #join <TT>join</TT>}. This call
     * unblocks that <TT>accept</TT>, releasing the <I>server</I> and <I>client</I>
     * to resume separate lives.
     */
    protected void fork() {
        c.write(null);
    }

    /**
     * This is one of the {@link Guard} methods needed by the {@link Alternative}
     * class.
     */
    @Override
    boolean enable(Alternative alt) {
        return c.readerEnable(alt);
    }

    /**
     * This is one of the {@link Guard} methods needed by the {@link Alternative}
     * class.
     */
    @Override
    boolean disable() {
        return c.readerDisable();
    }
}
