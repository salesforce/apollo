
//////////////////////////////////////////////////////////////////////
//                                                                  //
//  jcspDemos Demonstrations of the JCSP ("CSP for Java") Library   //
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

package extendedRendezvous;

import jcsp.lang.AltingBarrier;
import jcsp.lang.CSProcess;
import jcsp.lang.ChannelOutput;

import java.security.InvalidParameterException;
import java.util.Arrays;
import java.util.List;

/**
 * A process that writes out a list of values, synchronizing on the
 * corresponding barrier after each.
 * 
 * 
 * @author N.C.C. Brown
 *
 */
@SuppressWarnings("rawtypes")
public class WriterProcess implements CSProcess {

    private ChannelOutput<Object> out;

    private List<?> values;

    private AltingBarrier[][] events;

    @SuppressWarnings("unchecked")
    public WriterProcess(ChannelOutput out, List values, AltingBarrier[][] events) {
        if (values.size() != events.length) {
            throw new InvalidParameterException("Values must be the same length as Events");
        }

        this.out = out;
        this.values = values;
        this.events = events;
    }

    public WriterProcess(ChannelOutput<Object> out, List<?> values, AltingBarrier event) {
        this.out = out;
        this.values = values;
        this.events = new AltingBarrier[values.size()][];
        Arrays.fill(this.events, new AltingBarrier[] { event });
    }

    public void run() {

        for (int i = 0; i < events.length; i++) {
            AltingBarrier[] barriers = events[i];
            for (int j = 0; j < barriers.length; j++) {
                AltingBarrier barrier = barriers[j];
                if (barrier != null) {
                    barrier.mark();
                }
            }
        }

        for (int i = 0; i < values.size(); i++) {

            out.write(values.get(i));

            AltingBarrier[] barriers = events[i];
            for (int j = 0; j < barriers.length; j++) {
                AltingBarrier barrier = barriers[j];
                if (barrier != null) {
                    barrier.sync();
                }
            }
        }

    }

}
