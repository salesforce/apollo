
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

package jcsp.util;

/**
 * This is thrown if an attempt is made to create some variety of buffered
 * channel with a zero or negative sized buffer.
 *
 * <H2>Description</H2> Buffered channels must have (usually non-zero) positive
 * sized buffers. The following constructions will all throw this {@link Error}:
 * 
 * <pre>
 * One2OneChannel c = Channel.one2one(new Buffer(-42)); // must be &gt;= 0
 * One2OneChannel c = Channel.one2one(new OverFlowingBuffer(-42)); // must be &gt; 0
 * One2OneChannel c = Channel.one2one(new OverWriteOldestBuffer(-42)); // must be &gt; 0
 * One2OneChannel c = Channel.one2one(new OverWritingBuffer(-42)); // must be &gt; 0
 * One2OneChannel c = Channel.one2one(new InfiniteBuffer(-42)); // must be &gt; 0
 * </pre>
 * 
 * Zero-buffered non-overwriting channels are, of course, the default channel
 * semantics. The following constructions are all legal and equivalent:
 * 
 * <pre>
 * One2OneChannel c = Channel.one2one();
 * One2OneChannel c = Channel.one2one(new ZeroBuffer()); // less efficient
 * One2OneChannel c = Channel.one2one(new Buffer(0)); // less efficient
 * </pre>
 * 
 * No action should be taken to catch <TT>BufferSizeError</TT>. Application code
 * generating it is in error and needs correcting.
 *
 * @author P.H. Welch
 */

public class BufferSizeError extends Error {
    private static final long serialVersionUID = 1L;

    public BufferSizeError(String s) {
        super(s);
    }
}
