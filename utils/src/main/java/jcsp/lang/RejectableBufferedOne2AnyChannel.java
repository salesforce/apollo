
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

import jcsp.util.ChannelDataStore;

/**
 * <p>
 * This implements a one-to-any object channel with user-definable buffering,
 * safe for use by a single writer and many readers. Refer to
 * {@link One2AnyChannel} for a description of this behaviour.
 * </p>
 *
 * <p>
 * Additionally, this channel supports a <code>reject</code> operation. One of
 * the readers may call the reject method to force any current writer to abort
 * with a <code>ChannelDataRejectedException</code> (unless there is already a
 * read which will cause completion of the write). Subsequent read and write
 * attempts will immediately cause a <code>ChannelDataRejectedException</code>.
 * </p>
 *
 * @author Quickstone Technologies Limited
 * 
 * @deprecated This channel is superceded by the poison mechanisms, please see
 *             {@link PoisonException}
 */
@Deprecated
public class RejectableBufferedOne2AnyChannel<T> implements RejectableChannel<T> {
    BufferedOne2AnyChannel<T> innerChannel;

    /**
     * Constructs a new <code>RejectableBufferedOne2AnyChannel</code>
     *
     * @param data the buffer implementation to use.
     */
    public RejectableBufferedOne2AnyChannel(ChannelDataStore<T> data) {
        innerChannel = (BufferedOne2AnyChannel<T>) Channel.one2any(data);
    }

    @Override
    public RejectableChannelInput in() {
        return new RejectableChannelInputImpl<T>(innerChannel, 0);
    }

    @Override
    public RejectableChannelOutput out() {
        return new RejectableChannelOutputImpl<T>(innerChannel, 0);
    }

}
