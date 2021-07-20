
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

import jcsp.util.ints.ChannelDataStoreInt;

class PoisonableBufferedOne2OneChannelInt implements One2OneChannelInt, ChannelInternalsInt {

    /** The ChannelDataStore used to store the data for the channel */
    private final ChannelDataStoreInt data;

    private final Object rwMonitor = new Object();

    private Alternative alt;

    // Only passed to channel-ends, not used directly:
    private int immunity;

    private int poisonStrength = 0;

    /**
     * Constructs a new BufferedOne2OneChannel with the specified ChannelDataStore.
     *
     * @param data the ChannelDataStore used to store the data for the channel
     */
    public PoisonableBufferedOne2OneChannelInt(ChannelDataStoreInt data, int _immunity) {
        if (data == null)
            throw new IllegalArgumentException("Null ChannelDataStore given to channel constructor ...\n");
        this.data = (ChannelDataStoreInt) data.clone();
        immunity = _immunity;
    }

    private boolean isPoisoned() {
        return poisonStrength > 0;
    }

    /**
     * Reads an <TT>Object</TT> from the channel.
     *
     * @return the object read from the channel.
     */
    @Override
    public int read() {
        synchronized (rwMonitor) {

            if (data.getState() == ChannelDataStoreInt.EMPTY) {
                // Reader only sees poison if buffer is empty:
                if (isPoisoned()) {
                    throw new PoisonException(poisonStrength);
                }

                try {
                    rwMonitor.wait();
                    while (data.getState() == ChannelDataStoreInt.EMPTY && !isPoisoned()) {
                        if (Spurious.logging) {
                            SpuriousLog.record(SpuriousLog.One2OneChannelXRead);
                        }
                        rwMonitor.wait();
                    }
                } catch (InterruptedException e) {
                    throw new ProcessInterruptedException("*** Thrown from One2OneChannel.read (int)\n" + e.toString());
                }

                if (isPoisoned()) {
                    throw new PoisonException(poisonStrength);
                }
            }

            rwMonitor.notify();
            return data.get();
        }
    }

    @Override
    public int startRead() {
        synchronized (rwMonitor) {

            if (data.getState() == ChannelDataStoreInt.EMPTY) {
                // Reader only sees poison if buffer is empty:
                if (isPoisoned()) {
                    throw new PoisonException(poisonStrength);
                }
                try {
                    rwMonitor.wait();
                    while (data.getState() == ChannelDataStoreInt.EMPTY && !isPoisoned()) {
                        if (Spurious.logging) {
                            SpuriousLog.record(SpuriousLog.One2OneChannelXRead);
                        }
                        rwMonitor.wait();
                    }
                } catch (InterruptedException e) {
                    throw new ProcessInterruptedException("*** Thrown from One2OneChannel.read (int)\n" + e.toString());
                }

                // Reader only sees poison if buffer is empty:
                if (isPoisoned()) {
                    throw new PoisonException(poisonStrength);
                }
            }

            return data.startGet();
        }
    }

    @Override
    public void endRead() {
        synchronized (rwMonitor) {
            data.endGet();
            rwMonitor.notify();
        }
    }

    /**
     * Writes an <TT>Object</TT> to the channel.
     *
     * @param value the object to write to the channel.
     */
    @Override
    public void write(int value) {
        synchronized (rwMonitor) {
            // Writer always sees poison:
            if (isPoisoned()) {
                throw new PoisonException(poisonStrength);
            }

            data.put(value);
            if (alt != null) {
                alt.schedule();
            } else {
                rwMonitor.notify();
            }
            if (data.getState() == ChannelDataStoreInt.FULL) {
                try {
                    rwMonitor.wait();
                    while (data.getState() == ChannelDataStoreInt.FULL && !isPoisoned()) {
                        if (Spurious.logging) {
                            SpuriousLog.record(SpuriousLog.One2OneChannelXWrite);
                        }
                        rwMonitor.wait();
                    }
                } catch (InterruptedException e) {
                    throw new ProcessInterruptedException("*** Thrown from One2OneChannel.write (Object)\n"
                    + e.toString());
                }

                if (isPoisoned()) {
                    throw new PoisonException(poisonStrength);
                }
            }
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
            if (isPoisoned()) {
                // If it's poisoned, it will be ready whether because of the poison, or because
                // the buffer has data in it
                return true;
            } else if (data.getState() == ChannelDataStoreInt.EMPTY) {
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
            return data.getState() != ChannelDataStoreInt.EMPTY || isPoisoned();
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
            return (data.getState() != ChannelDataStoreInt.EMPTY) || isPoisoned();
        }
    }

    /**
     * Returns the <code>AltingChannelInput</code> to use for this channel. As
     * <code>BufferedOne2OneChannel</code> implements
     * <code>AltingChannelInput</code> itself, this method simply returns a
     * reference to the object that it is called on.
     *
     * @return the <code>AltingChannelInput</code> object to use for this channel.
     */
    @Override
    public AltingChannelInputInt in() {
        return new AltingChannelInputIntImpl(this, immunity);
    }

    /**
     * Returns the <code>ChannelOutput</code> object to use for this channel. As
     * <code>BufferedOne2OneChannel</code> implements <code>ChannelOutput</code>
     * itself, this method simply returns a reference to the object that it is
     * called on.
     *
     * @return the <code>ChannelOutput</code> object to use for this channel.
     */
    @Override
    public ChannelOutputInt out() {
        return new ChannelOutputIntImpl(this, immunity);
    }

    @Override
    public void writerPoison(int strength) {
        if (strength > 0) {
            synchronized (rwMonitor) {
                this.poisonStrength = strength;

                // Poison by writer does *NOT* clear the buffer

                rwMonitor.notifyAll();

                if (null != alt) {
                    alt.schedule();
                }
            }
        }
    }

    @Override
    public void readerPoison(int strength) {
        if (strength > 0) {
            synchronized (rwMonitor) {
                this.poisonStrength = strength;

                // Poison by reader clears the buffer:
                data.removeAll();

                rwMonitor.notifyAll();
            }
        }
    }

}
