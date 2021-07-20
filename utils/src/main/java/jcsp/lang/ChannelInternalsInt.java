
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

interface ChannelInternalsInt {

    public int read();

    public void write(int obj);

    public int startRead();

    public void endRead();

    public boolean readerEnable(Alternative alt);

    public boolean readerDisable();

    public boolean readerPending();

    /*
     * //For Symmetric channel, later: public boolean writerEnable(Alternative alt);
     * public boolean writerDisable(); public boolean writerPending();
     */

    public void readerPoison(int strength);

    public void writerPoison(int strength);

}
