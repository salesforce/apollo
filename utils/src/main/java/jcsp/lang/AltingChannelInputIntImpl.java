
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

class AltingChannelInputIntImpl extends AltingChannelInputInt {

    private ChannelInternalsInt channel;
    private int                 immunity;

    AltingChannelInputIntImpl(ChannelInternalsInt _channel, int _immunity) {
        channel = _channel;
        immunity = _immunity;
    }

    @Override
    public boolean pending() {
        return channel.readerPending();
    }

    @Override
    boolean disable() {
        return channel.readerDisable();
    }

    @Override
    boolean enable(Alternative alt) {
        return channel.readerEnable(alt);
    }

    @Override
    public void endRead() {
        channel.endRead();
    }

    @Override
    public int read() {
        return channel.read();
    }

    @Override
    public int startRead() {
        return channel.startRead();
    }

    @Override
    public void poison(int strength) {
        if (strength > immunity) {
            channel.readerPoison(strength);
        }
    }

}
