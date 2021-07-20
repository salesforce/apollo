
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

class AltingChannelInputSymmetricImpl<T> extends AltingChannelInput<T> implements MultiwaySynchronisation {

    private final AltingBarrier ab;

    private final ChannelInput<T> in;

    private boolean syncDone = false;

    public AltingChannelInputSymmetricImpl(AltingBarrier ab, ChannelInput<T> in) {
        this.ab = ab;
        this.in = in;
    }

    @Override
    boolean enable(Alternative alt) {
        syncDone = ab.enable(alt);
        return syncDone;
    }

    @Override
    boolean disable() {
        syncDone = ab.disable();
        return syncDone;
    }

    @Override
    public T read() {
        if (!syncDone)
            ab.sync();
        syncDone = false;
        return in.read();
    }

    @Override
    public boolean pending() {
        syncDone = ab.poll(10);
        return syncDone;
    }

    @Override
    public void poison(int strength) {
        in.poison(strength);
    }

    @Override
    public void endRead() {
        in.endRead();
    }

    @Override
    public T startRead() {
        if (!syncDone)
            ab.sync();
        syncDone = false;
        return in.read();
    }

}
