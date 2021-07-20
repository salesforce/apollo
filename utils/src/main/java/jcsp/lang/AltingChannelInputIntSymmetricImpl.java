
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

class AltingChannelInputIntSymmetricImpl extends AltingChannelInputInt implements MultiwaySynchronisation {

    private final AltingBarrier ab;

    private final ChannelInputInt in;

    private boolean syncDone = false;

    public AltingChannelInputIntSymmetricImpl(AltingBarrier ab, ChannelInputInt in) {
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
    public int read() {
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
    public int startRead() {
        if (!syncDone)
            ab.sync();
        syncDone = false;
        return in.read();
    }

}
