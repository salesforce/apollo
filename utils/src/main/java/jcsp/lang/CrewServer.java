
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

/**
 * @author P.H. Welch
 */
class CrewServer implements CSProcess {

    public static final int READER = 0;
    public static final int WRITER = 1;

    private final AltingChannelInputInt request;
    private final AltingChannelInputInt writerControl;
    private final AltingChannelInputInt readerRelease;

    /// TODO change this to use poisoning of the above channels, once poison is
    /// added
    private final AltingChannelInputInt poison;

    public CrewServer(final AltingChannelInputInt request, final AltingChannelInputInt writerControl,
                      final AltingChannelInputInt readerRelease, final AltingChannelInputInt poison) {
        this.request = request;
        this.writerControl = writerControl;
        this.readerRelease = readerRelease;
        this.poison = poison;
    }

    @Override
    public void run() {
        int nReaders = 0;
        final Alternative altMain = new Alternative(new Guard[] { readerRelease, request, poison });
        final int MAIN_READER_RELEASE = 0;
        final int MAIN_REQUEST = 1;
        final int MAIN_POISON = 2;
        final Alternative altWriteComplete = new Alternative(new Guard[] { writerControl, poison });
        final int WC_WRITER_CONTROL = 0;
        final int WC_POISON = 1;
        final Alternative altReadComplete = new Alternative(new Guard[] { readerRelease, poison });
        final int RC_READER_RELEASE = 0;
        final int RC_POISON = 1;
        while (true) {
            // invariant : (nReaders is the number of current readers) and (there are no
            // writers)
            switch (altMain.priSelect()) {
            case MAIN_READER_RELEASE:
                readerRelease.read();
                nReaders--;
                break;
            case MAIN_REQUEST:
                switch (request.read()) {
                case READER:
                    nReaders++;
                    break;
                case WRITER:
                    int n = nReaders; // tmp
                    for (int i = 0; i < n; i++) {
                        switch (altReadComplete.priSelect()) {
                        case RC_READER_RELEASE:
                            readerRelease.read();
                            nReaders--; // tmp
                            break;
                        case RC_POISON:
                            poison.read(); // let the finalizer complete
                            return;
                        }
                    }
                    nReaders = 0;
                    writerControl.read(); // let writer start writing
                    switch (altWriteComplete.priSelect()) {
                    case WC_WRITER_CONTROL:
                        writerControl.read(); // let writer finish writing
                        break;
                    case WC_POISON:
                        poison.read(); // let the finalizer complete
                        return;
                    }
                    break;
                }
                break;
            case MAIN_POISON:
                poison.read(); // let the finalizer complete
                return;
            }
        }
    }
}
