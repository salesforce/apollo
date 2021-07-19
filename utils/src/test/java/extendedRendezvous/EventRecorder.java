
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

import java.util.LinkedList;
import java.util.List;

import jcsp.lang.Alternative;
import jcsp.lang.CSProcess;
import jcsp.lang.Guard;

/**
 * A class that listens out for many guards, and records the order in which they
 * occur
 * 
 * Note: do not pass in channel guards, as the process will not perform the
 * necessary input after the guard is selected
 * 
 * @author nccb
 *
 */
class EventRecorder implements CSProcess {
    private Guard originalGuards[];

    private int stopOnGuard;

    private List<Guard> observedGuards = new LinkedList<>();

    public EventRecorder(Guard[] guards, int terminateEvent) {
        originalGuards = guards;
        stopOnGuard = terminateEvent;
    }

    public Guard[] getObservedEvents() {
        return (Guard[]) observedGuards.toArray(new Guard[observedGuards.size()]);
    }

    public void run() {
        Alternative alt = new Alternative(originalGuards);
        int selected;

        do {
            selected = alt.select();

            observedGuards.add(originalGuards[selected]);

        } while (selected != stopOnGuard);

    }

}
