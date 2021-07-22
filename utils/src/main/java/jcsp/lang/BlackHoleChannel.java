
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
 * This implements {@link ChannelOutput} with <I>black hole</I> semantics.
 * <H2>Description</H2> <TT>BlackHoleChannel</TT> is an implementation of
 * {@link ChannelOutput} that yields <I>black hole</I> semantics for the
 * channel. Writers may always write but there can be no readers. Any number of
 * writers may share the same <I>black hole</I>.
 * <P>
 * <I>Note:</I> <TT>BlackHoleChannel</TT>s are used for masking off unwanted
 * outputs from processes. They are useful when we want to reuse an existing
 * process component intact, but don't need some of its output channels (i.e. we
 * don't want to redesign and reimplement the component to remove the redundant
 * channels). Normal channels cannot be plugged in and left dangling as this may
 * deadlock (parts of) the component being reused.
 * <P>
 *
 * @see ChannelOutput
 * @see One2OneChannel
 * @see Any2OneChannel
 * @see One2AnyChannel
 * @see Any2AnyChannel
 *
 * @author P.H. Welch
 */

public class BlackHoleChannel<T> implements ChannelOutput<T> {
    /**
     * Write an Object to the channel and lose it.
     *
     * @param object the object to write to the channel.
     */
    @Override
    public void write(T object) {
    }

    @Override
    public void poison(int strength) {
    }
}
