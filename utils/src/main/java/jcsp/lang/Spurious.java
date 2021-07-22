
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
 * This holds the static flag (indicating whether spurious wakeups should be
 * logged) and early timeout allowance (for {@link Alternative}s with
 * {@link CSTimer} guards).
 * 
 * <H2>Description</H2> These fields are held in this separate class to minimise
 * class loading when spurious wakeups are not logged - the default condition.
 *
 * @see jcsp.lang.SpuriousLog
 *
 * @author P.H. Welch
 */
//}}}

class Spurious { // package-only visible

    /**
     * If logging is required, this flag should be set <i>before</i> any concurrency
     * is started. It should only be set <i>once</i> using
     * {@link SpuriousLog#start()}. There is no concurrency protection!
     */
    static public boolean logging = false;

    /**
     * This is the allowed early timeout (in msecs). Some JVMs timeout on calls of
     * <tt>wait (timeout)</tt> early - this specifies how early JCSP will tolerate.
     * <p>
     * We need this to distinguish between a <i>JVM-early</i> timeout (that should
     * be accepted) and a <i>spurious wakeup</i> (that should not). The value to
     * which this field should be set is machine dependant. For JVMs that do not
     * return early timeouts, it should be set to zero. For many, it should be left
     * at the default value (4). If {@link Spurious#logging} is enabled, counts of
     * spurious wakeups versus accepted early timeouts on <tt>select</tt> operations
     * on {@link Alternative}s can be obtained; this field should be set to minimise
     * the former.
     * <p>
     * This field should be set <i>before</i> any concurrency is started. It should
     * only be set <i>once</i> using {@link SpuriousLog#setEarlyTimeout(long)}.
     * There is no concurrency protection!
     */
    static public long earlyTimeout = 9;

}
