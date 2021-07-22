
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

class Any2OneImpl<T> implements ChannelInternals<T>, Any2OneChannel<T, T> {

    private ChannelInternals<T> channel;
    private final Object        writeMonitor = new Object();

    Any2OneImpl(ChannelInternals<T> _channel) {
        channel = _channel;
    }

    // Begin never used:
    @Override
    public void endRead() {
        channel.endRead();
    }

    @Override
    public T read() {
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
    public T startRead() {
        return channel.startRead();
    }
    // End never used

    @Override
    public void write(T obj) {
        synchronized (writeMonitor) {
            channel.write(obj);
        }

    }

    @Override
    public void writerPoison(int strength) {
        synchronized (writeMonitor) {
            channel.writerPoison(strength);
        }

    }

    @Override
    public AltingChannelInput<T> in() {
        return new AltingChannelInputImpl<T>(channel, 0);
    }

    @Override
    public SharedChannelOutput<T> out() {
        return new SharedChannelOutputImpl<T>(this, 0);
    }

}
