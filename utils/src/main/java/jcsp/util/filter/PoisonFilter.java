
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

package jcsp.util.filter;

/**
 * This filter will throw a <code>PoisonException</code> when
 * <code>filter(Object)</code> is called. This can be used to prevent a channel
 * from being written to or read from.
 *
 * @author Quickstone Technologies Limited
 */
public class PoisonFilter implements Filter {
    /**
     * The message to be placed in the <code>PoisonException</code> raised.
     */
    private String message;

    /**
     * Default message.
     */
    private static String defaultMessage = "Channel end has been poisoned.";

    /**
     * Constructs a new filter with the default message.
     */
    public PoisonFilter() {
        this(defaultMessage);
    }

    /**
     * Constructs a new filter with a specific message.
     */
    public PoisonFilter(String message) {
        this.message = message;
    }

    @Override
    public Object filter(Object obj) {
        throw new PoisonFilterException(this.message);
    }
}
