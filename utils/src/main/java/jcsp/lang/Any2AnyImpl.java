
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

class Any2AnyImpl<T> implements Any2AnyChannel<T, T>, ChannelInternals<T> {

    private ChannelInternals<T> channel;
    /** The mutex on which readers must synchronize */
    private final Mutex         readMutex    = new Mutex();
    private final Object        writeMonitor = new Object();

    Any2AnyImpl(ChannelInternals<T> _channel) {
        channel = _channel;
    }

    @Override
    public SharedChannelInput<T> in() {
        return new SharedChannelInputImpl<T>(this, 0);
    }

    @Override
    public SharedChannelOutput<T> out() {
        return new SharedChannelOutputImpl<T>(this, 0);
    }

    @Override
    public void endRead() {
        channel.endRead();
        readMutex.release();

    }

    @Override
    public T read() {
        readMutex.claim();
//                A poison exception might be thrown, hence the try/finally:                
        try {
            return channel.read();
        } finally {
            readMutex.release();
        }
    }

    // begin never used:
    @Override
    public boolean readerDisable() {
        return false;
    }

    @Override
    public boolean readerEnable(Alternative alt) {
        return false;
    }

    @Override
    public boolean readerPending() {
        return false;
    }
    // end never used

    @Override
    public void readerPoison(int strength) {
        readMutex.claim();
        channel.readerPoison(strength);
        readMutex.release();
    }

    @Override
    public T startRead() {
        readMutex.claim();
        try {
            return channel.startRead();
        } catch (RuntimeException e) {
            channel.endRead();
            readMutex.release();
            throw e;
        }

    }

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

}
