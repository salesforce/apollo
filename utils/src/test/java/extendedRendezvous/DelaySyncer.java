
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
import jcsp.lang.CSTimer;

public class DelaySyncer implements CSProcess {

  AltingBarrier barrier;
  CSTimer timer;
  int milliSeconds;
  int iterations;
  
  public DelaySyncer(AltingBarrier barrier, int milliSeconds, int iterations) {
    this.barrier = barrier;
    timer = new CSTimer();
    this.milliSeconds = milliSeconds;
    this.iterations = iterations;     
  }
  
  public void run() {
    
    barrier.mark();
    
    for (int i = 0;i < iterations;i++) {
      timer.sleep(milliSeconds);
      barrier.sync();
    }

  }
  
  

}
