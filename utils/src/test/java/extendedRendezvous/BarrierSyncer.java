
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

/**
 * A process that syncs on one alting barrier and finishes
 * 
 * 
 * @author N.C.C. Brown
 *
 */
public class BarrierSyncer implements CSProcess {

  private AltingBarrier barrier;
  
  public BarrierSyncer(AltingBarrier barrier) {
    super();
    this.barrier = barrier;
  }



  public void run() {
    
    barrier.mark();
    
    barrier.sync();
    
  }

}
