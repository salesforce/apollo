
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

class SharedChannelOutputImpl<T> implements SharedChannelOutput<T> {

    private ChannelInternals<T> channel;
    private int                 immunity;

    SharedChannelOutputImpl(ChannelInternals<T> _channel, int _immunity) {
        channel = _channel;
        immunity = _immunity;
    }

    @Override
    public void write(T object) {
        channel.write(object);

    }

    @Override
    public void poison(int strength) {
        if (strength > immunity) {
            channel.writerPoison(strength);
        }
    }

}
