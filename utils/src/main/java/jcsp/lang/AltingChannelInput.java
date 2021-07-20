
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
 * This extends {@link Guard} and {@link ChannelInput} to enable a process to
 * choose between many object input (and other) events.
 * <p>
 * A <i>reading-end</i>, obtained from a <i>one-one</i> or <i>any-one</i>
 * channel by invoking its <tt>in()</tt> method, will extend this abstract
 * class.
 * <H2>Description</H2> <TT>AltingChannelInput</TT> extends {@link Guard} and
 * {@link ChannelInput} to enable a process to choose between many object input
 * (and other) events. The methods inherited from <TT>Guard</TT> are of no
 * concern to users of this package.
 * </P>
 * <H2>Example</H2>
 * 
 * <PRE>
 * import jcsp.lang.*;
 * <I></I>
 * public class AltingExample implements CSProcess {
 * <I></I>
 *   private final AltingChannelInput in0, in1;
 *   <I></I>
 *   public AltingExample (final AltingChannelInput in0,
 *                         final AltingChannelInput in1) {
 *     this.in0 = in0;
 *     this.in1 = in1;
 *   }
 * <I></I>
 *   public void run () {
 * <I></I>
 *     final Guard[] altChans = {in0, in1};
 *     final Alternative alt = new Alternative (altChans);
 * <I></I>
 *     while (true) {
 *       switch (alt.select ()) {
 *         case 0:
 *           System.out.println ("in0 read " + in0.read ());
 *         break;
 *         case 1:
 *           System.out.println ("in1 read " + in1.read ());
 *         break;
 *       }
 *     }
 * <I></I>
 *   }
 * <I></I>
 * }
 * </PRE>
 *
 * @see Guard
 * @see Alternative
 * @author P.D. Austin and P.H. Welch
 */

public abstract class AltingChannelInput<T> extends Guard implements ChannelInput<T> {
    // nothing alse to add ... except ...

    /**
     * Returns whether there is data pending on this channel.
     * <P>
     * <I>Note: if there is, it won't go away until you read it. But if there isn't,
     * there may be some by the time you check the result of this method.</I>
     *
     * @return state of the channel.
     */
    public abstract boolean pending();
}
