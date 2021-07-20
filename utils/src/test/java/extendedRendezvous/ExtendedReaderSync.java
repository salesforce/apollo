
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
import jcsp.lang.ChannelInput;

import java.security.InvalidParameterException;
import java.util.Arrays;

/**
 * A process that performs a set number of extended inputs, syncing on a barrier
 * as its extended action for each
 * 
 * @author N.C.C. Brown
 *
 */
public class ExtendedReaderSync implements CSProcess {

  private AltingBarrier[][] events;
  
  private ChannelInput<?> input;
  
  private int iterations;
  
  private Object[] valuesRead;
  
  public ExtendedReaderSync(AltingBarrier[][] barriers, ChannelInput<?> in, int iterations) {
    if (barriers.length != iterations) {
      throw new InvalidParameterException("Barriers must be the same length as iterations");
    }
    
    this.events = barriers;
    this.input = in;
    this.iterations = iterations;
    valuesRead = new Object[iterations];
  }
  
  public ExtendedReaderSync(AltingBarrier barrier, ChannelInput<?> in, int iterations) {
    this.events = new AltingBarrier[iterations][];
    Arrays.fill(this.events,new AltingBarrier[] {barrier});
    
    this.input = in;
    this.iterations = iterations;
    valuesRead = new Object[iterations];
  }
  
  public void run() {

    for (int i = 0;i < events.length;i++) {
      AltingBarrier[] barriers = events[i];
      for (int j = 0;j < barriers.length;j++) {
        AltingBarrier barrier = barriers[j];
        if (barrier != null) {
          barrier.mark();
        }
      }
    }
    
    for (int i = 0;i < iterations;i++) {
      valuesRead[i] = input.startRead();
      
      AltingBarrier[] barriers = events[i];
      if (barriers.length > 0) {
        AltingBarrier barrier = barriers[0];
        if (barrier != null) {
          barrier.sync();
        }
      }      
      
      input.endRead();
      
      if (barriers.length > 1) {
        AltingBarrier barrier = barriers[1];
        if (barrier != null) {
          barrier.sync();
        }
      }
    }

  }

  public Object[] getValuesRead() {
    return valuesRead;
  }

  
}
