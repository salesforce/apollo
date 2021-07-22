
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
 * This extends {@link Guard} and {@link ChannelOutputInt} to enable a process
 * to choose between many integer output (and other) events.
 * <p>
 * A <i>writing-end</i>, obtained only from a {@link One2OneChannelSymmetricInt
 * <i>symmetric</i>} channel by invoking its <tt>out()</tt> method, will
 * implement this interface.
 * <H2>Description</H2> <TT>AltingChannelOutputInt</TT> extends {@link Guard}
 * and {@link ChannelOutputInt} to enable a process to choose between many
 * integer output (and other) events. The methods inherited from <TT>Guard</TT>
 * are of no concern to users of this package.
 * </P>
 * <H2>Example</H2>
 * 
 * <PRE>
 * import jcsp.lang.*;
 * 
 * public class AltingOutputIntExample implements CSProcess {
 * 
 *     private final AltingChannelOutputInt out0, out1;
 * 
 *     public AltingOutputIntExample(final AltingChannelOutputInt out0, final AltingChannelOutputInt out1) {
 *         this.out0 = out0;
 *         this.out1 = out1;
 *     }
 * 
 *     public void run() {
 * 
 *         final Guard[] altChans = { out0, out1 };
 *         final Alternative alt = new Alternative(altChans);
 * 
 *         while (true) {
 *             switch (alt.select()) {
 *             case 0:
 *                 out0.write(0);
 *                 System.out.println("out0 written");
 *                 break;
 *             case 1:
 *                 out1.write(1);
 *                 System.out.println("out1 written");
 *                 break;
 *             }
 *         }
 * 
 *     }
 * 
 * }
 * </PRE>
 *
 * @see Guard
 * @see Alternative
 * @see One2OneChannelSymmetricInt
 * @see AltingChannelOutput
 * @author P.H. Welch
 */
//}}}

public abstract class AltingChannelOutputInt extends Guard implements ChannelOutputInt {

    // nothing alse to add

    /**
     * Returns whether the receiver is committed to read from this channel.
     * <P>
     * <I>Note: if this returns true, you must commit to write down this
     * channel.</I>
     *
     * @return state of the channel.
     */
    public abstract boolean pending();

}
