
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
 * This is the super-class for all {@link Alternative} events selectable by a
 * process.
 * <H2>Description</H2> <TT>Guard</TT> defines an abstract interface to be
 * implemented by events competing for selection by a process executing an
 * {@link Alternative}. Its methods have only <I>package</I> visibility within
 * <TT>jcsp.lang</TT> and are of no concern to <I>users</I> of this package.
 * Currently, JCSP supports channel inputs, accepts, timeouts and skips as
 * guards.
 * <P>
 * <I>Note: for those familiar with the <I><B>occam</B></I> multiprocessing
 * language, classes implementing </I><TT>Guard</TT><I> correspond to process
 * guards for use within </I><TT>ALT</TT><I> constructs.</I>
 *
 * @see jcsp.lang.CSTimer
 * @see Skip
 * @see jcsp.lang.AltingChannelInput
 * @see jcsp.lang.AltingChannelInputInt
 * @see Alternative
 * @author P.D. Austin
 * @author P.H. Welch
 */

public abstract class Guard {
    /**
     * Returns true if the event is ready. Otherwise, this enables the guard for
     * selection and returns false.
     * <P>
     * <I>Note: this method should only be called by the Alternative class</I>
     *
     * @param alt the Alternative class that is controlling the selection
     * @return true if and only if the event is ready
     */
    abstract boolean enable(Alternative alt);

    /**
     * Disables the guard for selection. Returns true if the event was ready.
     * <P>
     * <I>Note: this method should only be called by the Alternative class</I>
     *
     * @return true if and only if the event was ready
     */
    abstract boolean disable();

    /**
     * Schedules the process performing the given Alternative to run again. This is
     * intended for use by advanced users of the library who want to create their
     * own Guards that are not in the jcsp.lang package.
     * 
     * @param alt The Alternative to schedule
     */
    protected void schedule(Alternative alt) {
        alt.schedule();
    }
}
