
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

class Any2OneIntImpl implements ChannelInternalsInt, Any2OneChannelInt {

    private ChannelInternalsInt channel;
    private final Object        writeMonitor = new Object();

    Any2OneIntImpl(ChannelInternalsInt _channel) {
        channel = _channel;
    }

    // Begin never used:
    @Override
    public void endRead() {
        channel.endRead();
    }

    @Override
    public int read() {
        return channel.read();
    }

    @Override
    public boolean readerDisable() {
        return channel.readerDisable();
    }

    @Override
    public boolean readerEnable(Alternative alt) {
        return channel.readerEnable(alt);
    }

    @Override
    public boolean readerPending() {
        return channel.readerPending();
    }

    @Override
    public void readerPoison(int strength) {
        channel.readerPoison(strength);

    }

    @Override
    public int startRead() {
        return channel.startRead();
    }
    // End never used

    @Override
    public void write(int n) {
        synchronized (writeMonitor) {
            channel.write(n);
        }

    }

    @Override
    public void writerPoison(int strength) {
        synchronized (writeMonitor) {
            channel.writerPoison(strength);
        }

    }

    @Override
    public AltingChannelInputInt in() {
        return new AltingChannelInputIntImpl(channel, 0);
    }

    @Override
    public SharedChannelOutputInt out() {
        return new SharedChannelOutputIntImpl(this, 0);
    }

}
