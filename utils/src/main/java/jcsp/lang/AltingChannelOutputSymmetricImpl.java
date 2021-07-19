

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

  class AltingChannelOutputSymmetricImpl extends AltingChannelOutput
  implements MultiwaySynchronisation {

  private final AltingBarrier ab;

  private final ChannelOutput out;

  private boolean syncDone = false;
  
  public AltingChannelOutputSymmetricImpl (
          AltingBarrier ab, ChannelOutput out) {
    this.ab = ab;
    this.out = out;
  }

  boolean enable (Alternative alt) {
    syncDone = ab.enable (alt);
    return syncDone;
  }

  boolean disable () {
    syncDone = ab.disable ();
    return syncDone;
  }

  public void write (Object o) {
    if (!syncDone) ab.sync ();
    syncDone = false;
    out.write (o);
  }

  public boolean pending () {
    syncDone = ab.poll (10);
    return syncDone;
  }

  public void poison(int strength) {
	out.poison(strength);
  }
  
  

}
