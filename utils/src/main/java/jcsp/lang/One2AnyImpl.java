
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

    class One2AnyImpl implements One2AnyChannel, ChannelInternals {

	private ChannelInternals channel;
	/** The mutex on which readers must synchronize */
    private final Mutex readMutex = new Mutex();
    
    One2AnyImpl(ChannelInternals _channel) {
		channel = _channel;
	}
	
	public SharedChannelInput in() {
		return new SharedChannelInputImpl(this,0);
	}

	public ChannelOutput out() { 
		return new ChannelOutputImpl(channel,0);
	}

	public void endRead() {
		channel.endRead();
		readMutex.release();

	}

	public Object read() {
		readMutex.claim();
		//A poison exception might be thrown, hence the try/finally:		
		try
		{
			return channel.read();
		}
		finally
		{
			readMutex.release();		
		}		
	}

	//begin never used:
	public boolean readerDisable() {
		return false;
	}

	public boolean readerEnable(Alternative alt) {
		return false;
	}

	public boolean readerPending() {
		return false;
	}
	//end never used

	public void readerPoison(int strength) {
		readMutex.claim();
		channel.readerPoison(strength);
		readMutex.release();
	}

	public Object startRead() {
		readMutex.claim();		
		try
		{
			return channel.startRead();
		}
		catch (RuntimeException e)
		{
			channel.endRead();
			readMutex.release();
			throw e;
		}
		
	}

	//begin never used
	public void write(Object obj) {
		channel.write(obj);
	}

	public void writerPoison(int strength) { 
		channel.writerPoison(strength);
	}
	//end never used

}
