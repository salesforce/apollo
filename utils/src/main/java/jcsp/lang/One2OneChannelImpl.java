
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
 * This implements a one-to-one object channel.
 * <H2>Description</H2> <TT>One2OneChannelImpl</TT> implements a one-to-one
 * object channel. Multiple readers or multiple writers are not allowed -- these
 * are catered for by {@link Any2OneChannelImpl}, {@link One2AnyChannelImpl} or
 * {@link Any2AnyChannelImpl}.
 * <P>
 * The reading process may {@link Alternative <TT>ALT</TT>} on this channel. The
 * writing process is committed (i.e. it may not back off).
 * <P>
 * The default semantics of the channel is that of CSP -- i.e. it is
 * zero-buffered and fully synchronised. The reading process must wait for a
 * matching writer and vice-versa.
 * <P>
 * However, the static methods of {@link Channel} allow the construction of a
 * channel with a <I>plug-in</I> driver conforming to the
 * {@link jcsp.util.ChannelDataStore <TT>ChannelDataStore</TT>} interface. This
 * allows a variety of different channel semantics to be introduced -- including
 * buffered channels of user-defined capacity (including infinite), overwriting
 * channels (with various overwriting policies) etc.. Standard examples are
 * given in the <TT>jcsp.util</TT> package, but <I>careful users</I> may write
 * their own.
 *
 * @see Alternative
 * @see Any2OneChannelImpl
 * @see One2AnyChannelImpl
 * @see Any2AnyChannelImpl
 * @see jcsp.util.ChannelDataStore
 *
 * @author P.D. Austin
 * @author P.H. Welch
 */

class One2OneChannelImpl<T> implements One2OneChannel<T, T>, ChannelInternals<T> {
    /** The monitor synchronising reader and writer on this channel */
    private Object rwMonitor = new Object();

    /** The (invisible-to-users) buffer used to store the data for the channel */
    private T hold;

    /** The synchronisation flag */
    private boolean empty = true;

    /** The Alternative class that controls the selection */
    private Alternative alt;

    /** Flag to deal with a spurious wakeup during a write */
    private boolean spuriousWakeUp = true;

    /************* Methods from One2OneChannel ******************************/

    /**
     * Returns the <code>AltingChannelInput</code> to use for this channel. As
     * <code>One2OneChannelImpl</code> implements <code>AltingChannelInput</code>
     * itself, this method simply returns a reference to the object that it is
     * called on.
     *
     * @return the <code>AltingChannelInput</code> object to use for this channel.
     */
    @Override
    public AltingChannelInput<T> in() {
        return new AltingChannelInputImpl<T>(this, 0);
    }

    /**
     * Returns the <code>ChannelOutput</code> object to use for this channel. As
     * <code>One2OneChannelImpl</code> implements <code>ChannelOutput</code> itself,
     * this method simply returns a reference to the object that it is called on.
     *
     * @return the <code>ChannelOutput</code> object to use for this channel.
     */
    @Override
    public ChannelOutput<T> out() {
        return new ChannelOutputImpl<T>(this, 0);
    }

    /************* Methods from ChannelOutput *******************************/

    /**
     * Writes an <TT>Object</TT> to the channel.
     *
     * @param value the object to write to the channel.
     */
    @Override
    public void write(T value) {
        synchronized (rwMonitor) {
            hold = value;
            if (empty) {
                empty = false;
                if (alt != null) {
                    alt.schedule();
                }
            } else {
                empty = true;
                rwMonitor.notify();
            }
            try {
                rwMonitor.wait();
                while (spuriousWakeUp) {
                    if (Spurious.logging) {
                        SpuriousLog.record(SpuriousLog.One2OneChannelWrite);
                    }
                    rwMonitor.wait();
                }
                spuriousWakeUp = true;
            } catch (InterruptedException e) {
                throw new ProcessInterruptedException("*** Thrown from One2OneChannel.write (Object)\n" + e.toString());
            }
        }
    }

    /** ***********Methods from AltingChannelInput************************* */

    /**
     * Reads an <TT>Object</TT> from the channel.
     *
     * @return the object read from the channel.
     */
    @Override
    public T read() {
        synchronized (rwMonitor) {
            if (empty) {
                empty = false;
                try {
                    rwMonitor.wait();
                    while (!empty) {
                        if (Spurious.logging) {
                            SpuriousLog.record(SpuriousLog.One2OneChannelRead);
                        }
                        rwMonitor.wait();
                    }
                } catch (InterruptedException e) {
                    throw new ProcessInterruptedException("*** Thrown from One2OneChannel.read ()\n" + e.toString());
                }
            } else {
                empty = true;
            }
            spuriousWakeUp = false;
            rwMonitor.notify();
            return hold;
        }
    }

    @Override
    public T startRead() {
        synchronized (rwMonitor) {
            if (empty) {
                empty = false;
                try {
                    rwMonitor.wait();
                    while (!empty) {
                        if (Spurious.logging) {
                            SpuriousLog.record(SpuriousLog.One2OneChannelRead);
                        }
                        rwMonitor.wait();
                    }
                } catch (InterruptedException e) {
                    throw new ProcessInterruptedException("*** Thrown from One2OneChannel.read ()\n" + e.toString());
                }
            } else {
                empty = true;
            }

            return hold;
        }
    }

    @Override
    public void endRead() {
        synchronized (rwMonitor) {
            spuriousWakeUp = false;
            rwMonitor.notify();
        }
    }

    /**
     * turns on Alternative selection for the channel. Returns true if the channel
     * has data that can be read immediately.
     * <P>
     * <I>Note: this method should only be called by the Alternative class</I>
     *
     * @param alt the Alternative class which will control the selection
     * @return true if the channel has data that can be read, else false
     */

    @Override
    public boolean readerEnable(Alternative alt) {
        synchronized (rwMonitor) {
            if (empty) {
                this.alt = alt;
                return false;
            } else {
                return true;
            }
        }
    }

    /**
     * turns off Alternative selection for the channel. Returns true if the channel
     * contained data that can be read.
     * <P>
     * <I>Note: this method should only be called by the Alternative class</I>
     *
     * @return true if the channel has data that can be read, else false
     */
    @Override
    public boolean readerDisable() {
        synchronized (rwMonitor) {
            alt = null;
            return !empty;
        }
    }

    /**
     * Returns whether there is data pending on this channel.
     * <P>
     * <I>Note: if there is, it won't go away until you read it. But if there isn't,
     * there may be some by the time you check the result of this method.</I>
     * <P>
     * This method is provided for convenience. Its functionality can be provided by
     * <I>Pri Alting</I> the channel against a <TT>SKIP</TT> guard, although at
     * greater run-time and syntactic cost. For example, the following code
     * fragment:
     * 
     * <PRE>
     *   if (c.pending ()) {
     *     Object x = c.read ();
     *     ...  do something with x
     *   } else (
     *     ...  do something else
     *   }
     * </PRE>
     * 
     * is equivalent to:
     * 
     * <PRE>
     *   if (c_pending.priSelect () == 0) {
     *     Object x = c.read ();
     *     ...  do something with x
     *   } else (
     *     ...  do something else
     * }
     * </PRE>
     * 
     * where earlier would have had to have been declared:
     * 
     * <PRE>
     * final Alternative c_pending = new Alternative(new Guard[] { c, new Skip() });
     * </PRE>
     *
     * @return state of the channel.
     */
    @Override
    public boolean readerPending() {
        synchronized (rwMonitor) {
            return !empty;
        }
    }

    // No poison in these channels:
    @Override
    public void writerPoison(int strength) {
    }

    @Override
    public void readerPoison(int strength) {
    }
}
