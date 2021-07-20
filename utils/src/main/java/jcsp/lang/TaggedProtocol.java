
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
 * <TT>TaggedProtocol</TT> is the base class for messages carrying an
 * <I><B>occam2</B></I>-like tagged (<TT>CASE</TT>) protocol over JCSP channels.
 * <P>
 * <H2>Example</H2>
 * <H3>Protocol Definition</H3> <TT>SampleProtocol</TT> illustrates one approach
 * to the passing of different information structures across the same JCSP
 * channel. It corresponds to the tagged (<TT>CASE</TT>) <TT>PROTOCOL</TT> of
 * <B>occam2.1</B>. The example was invented by Rick Beton (of Roke Manor
 * Research Ltd.):
 *
 * <PRE>
 * PROTOCOL String IS BYTE::[]BYTE:  -- slight licence here!
 * <I></I>
 * DATA TYPE Complex32
 *   RECORD
 *     REAL32 real, imag:
 * :
 * <I></I>
 * PROTOCOL SampleProtocol
 *   CASE
 *     NOTHING.IN.PARTICULAR
 *     STRING; String
 *     INTEGER; INT
 *     COMPLEX; Complex32
 *     BOO.TO.A.GOOSE
 * :
 * </PRE>
 *
 * <A NAME="SampleProtocol"> This corresponds to the (JCSP) <B>Java</B>
 * declaration:
 *
 * <PRE>
 * public class SampleProtocol {
 * <I></I>
 *   public static final int NOTHING_IN_PARTICULAR = 0;
 *   public static final int                STRING = 1;
 *   public static final int               INTEGER = 2;
 *   public static final int               COMPLEX = 3;
 *   public static final int        BOO_TO_A_GOOSE = 4;
 * <I></I>
 *   static public class NothingInParticular extends TaggedProtocol {
 *     public NothingInParticular () {
 *       super (NOTHING_IN_PARTICULAR);
 *     }
 *   }
 * <I></I>
 *   static public class String extends TaggedProtocol {
 *     public final java.lang.String string;
 *     public String (final java.lang.String string) {
 *       super (STRING);
 *       this.string = string;
 *     }
 *   }
 * <I></I>
 *   static public class Integer extends TaggedProtocol {
 *     public final int value;
 *     public Integer (final int value) {
 *       super (INTEGER);
 *       this.value = value;
 *     }
 *   }
 * <I></I>
 *   static public class Complex extends TaggedProtocol {
 *     public final float real;
 *     public final float imag;
 *     public Complex (final float real, final float imag) {
 *       super (COMPLEX);
 *       this.real = real;
 *       this.imag = imag;
 *     }
 *   }
 * <I></I>
 *   static public class BooToAGoose extends TaggedProtocol {
 *     public BooToAGoose () {
 *       super (BOO_TO_A_GOOSE);
 *     }
 *   }
 * <I></I>
 * }
 * </PRE>
 * 
 * The emphasis in the above definition is <I>security</I>. The protocol
 * variants hold only immutable data (whose transmission, therefore, can never
 * lead to race hazards). Secondly, it is impossible for the user of the
 * protocol to set up an incorrect tag or misinterpret a correct one without
 * raising an exception.
 *
 * <H3>Protocol Use</H3> This example has a simple pair of CSProcesses
 * communicating over a channel using the <TT>SampleProtocol</TT>.
 * <P>
 * First, here is the <B>occam2.1</B> version. The network is defined (and
 * started) by:
 * 
 * <PRE>
 * CHAN OF SampleProtocol c:
 * <I></I>
 * PAR
 *   Producer (c)
 *   Consumer (c, screen)
 * </PRE>
 * 
 * where:
 * 
 * <PRE>
 * PROC Producer (CHAN OF SampleProtocol out)
 * <I></I>
 *   VAL []BYTE m1 IS "Hello World ...":
 *   VAL []BYTE m2 IS "Goodbye World ...":
 * <I></I>
 *   SEQ
 *     out ! NOTHING.IN.PARTICULAR
 *     out ! STRING; (BYTE SIZE m1)::m1
 *     out ! INTEGER; 42
 *     out ! NOTHING.IN.PARTICULAR
 *     out ! COMPLEX; [3.1416, 0.7071]
 *     out ! STRING; (BYTE SIZE m2)::m2
 *     out ! BOO.TO.A.GOOSE
 * <I></I>
 * :
 * </PRE>
 * 
 * and where:
 * 
 * <PRE>
 * PROC Consumer (CHAN OF SampleProtocol in, CHAN OF BYTE screen)
 * <I></I>
 *   INITIAL BOOL running IS TRUE:
 * <I></I>
 *   WHILE running
 *     in ? CASE
 *       NOTHING.IN.PARTICULAR
 *         out.string ("==> NothingInParticular happening ...*c*n", 0, screen)
 *       BYTE size:
 *       [255]BYTE s:
 *       STRING; size::s
 *         SEQ
 *           out.string ("==> *"", 0, screen)
 *           out.string ([s FOR INT size], 0, screen)
 *           out.string ("*"*c*n", 0, screen)
 *       INT i:
 *       INTEGER; i
 *         SEQ
 *           out.string ("==> ", 0, screen)
 *           out.number (i, 0, screen)
 *           out.string ("*c*n", 0, screen)
 *       Complex32 c:
 *       COMPLEX; c
 *         SEQ
 *           out.string ("==> [", 0, screen)
 *           out.real32 (c[real], 0, screen)
 *           out.string (", ", 0, screen)
 *           out.real32 (c[imag], 0, screen)
 *           out.string ("*c*n", 0, screen)
 *       BOO.TO.A.GOOSE
 *         SEQ
 *           out.string ("==> Waaaaaa!  You scared me!!  I'm off!!!", 0, screen)
 *           running := FALSE
 * <I></I>
 * :
 * </PRE>
 * <P>
 * Here is the (JCSP) <B>Java</B> version. The network is defined (and started)
 * by:
 * 
 * <PRE>
 * final One2OneChannel c = Channel.one2one ();
 * <I></I>
 * new Parallel (
 *   new CSProcess[] {
 *      new Producer (c),
 *      new Consumer (c)
 *    }
 * ).run ();
 * </PRE>
 * 
 * <A NAME="Producer"> where:
 * 
 * <PRE>
 * import jcsp.lang.*;
 * <I></I>
 * public class Producer implements CSProcess {
 * <I></I>
 *   private final ChannelOutput out;
 * <I></I>
 *   public Producer (ChannelOutput out) {
 *     this.out = out;
 *   }
 * <I></I>
 *   public void run() {
 * <I></I>
 *     out.write (new SampleProtocol.NothingInParticular ());
 *     out.write (new SampleProtocol.String ("Hello World ..."));
 *     out.write (new SampleProtocol.Integer (42));
 *     out.write (new SampleProtocol.NothingInParticular ());
 *     out.write (new SampleProtocol.Complex (3.1416f, 0.7071f));
 *     out.write (new SampleProtocol.String ("Goodbye World ..."));
 *     out.write (new SampleProtocol.BooToAGoose ());
 * <I></I>
 *   }
 * <I></I>
 * }
 * </PRE>
 * 
 * <A NAME="Consumer"> and where:
 * 
 * <PRE>
 * import jcsp.lang.*;
 * <I></I>
 * public class Consumer implements CSProcess {
 * <I></I>
 *   private final ChannelInput in;
 * <I></I>
 *   public Consumer (ChannelInput in) {
 *     this.in = in;
 *   }
 * <I></I>
 *   public void run () {
 * <I></I>
 *     boolean running = true;
 * <I></I>
 *     while (running) {
 * <I></I>
 *       TaggedProtocol tp = (TaggedProtocol) in.read ();
 * <I></I>
 *       switch (tp.tag) {
 *          case SampleProtocol.NOTHING_IN_PARTICULAR:
 *            System.out.println ("==> NothingInParticular happening ...");
 *          break;
 *          case SampleProtocol.STRING:
 *            SampleProtocol.String sms = (SampleProtocol.String) tp;
 *            System.out.println ("==> \"" + sms.string + "\"");
 *          break;
 *          case SampleProtocol.INTEGER:
 *            SampleProtocol.Integer smi = (SampleProtocol.Integer) tp;
 *            System.out.println ("==> " + smi.value);
 *          break;
 *          case SampleProtocol.COMPLEX:
 *            SampleProtocol.Complex smc = (SampleProtocol.Complex) tp;
 *            System.out.println ("==> [" + smc.real + ", " + smc.imag + "]");
 *          break;
 *          case SampleProtocol.BOO_TO_A_GOOSE:
 *            System.out.println ("==> Waaaaaa!  You scared me!!  I'm off!!!");
 *            running = false;
 *          break;
 *       }
 * <I></I>
 *     }
 * <I></I>
 *   }
 * <I></I>
 * }
 * </PRE>
 *
 * @author P.H. Welch
 */

public class TaggedProtocol {
    /**
     * This public tag is used by the receiving process to determine which variant
     * of a tagged protocol has been received. See the above
     * <A HREF="#Consumer">Consumer</A> example (and the definition of its input
     * channel's <A HREF="#SampleProtocol">SampleProtocol</A>).
     */
    public final int tag;

    /**
     * This super-constructor is invoked by the extending sub-class constructor. It
     * should be passed a tag that is unique for the tagged protocol for which that
     * sub-class is one variant. See the above
     * <A HREF="#SampleProtocol">SampleProtocol</A> (and its use in
     * <A HREF="#Producer">Producer</A> and <A HREF="#Consumer">Consumer</A>).
     */
    public TaggedProtocol(final int tag) {
        this.tag = tag;
    }
}
