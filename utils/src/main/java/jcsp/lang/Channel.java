
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
import jcsp.util.ints.ChannelDataStoreInt;

/**
 * <p>
 * This class provides static factory methods for constructing all the different
 * types of channel.
 * </p>
 * <p>
 * Channels carry either <i>Objects</i> or <i>integers</i>.
 * </p>
 * <p>
 * Basic channels are zero-buffered: the writer and reader processes must
 * synchronise. Buffered channels can be made with various buffering policies:
 * e.g. fixed size blocking FIFO ({@link jcsp.util.Buffer <i>here</i>}), fixed
 * size <i>overwrite-oldest-when-full</i>
 * ({@link jcsp.util.OverWriteOldestBuffer <i>here</i>}), fixed size
 * <i>discard-when-full</i> ({@link jcsp.util.OverFlowingBuffer <i>here</i>}),
 * infinite sized FIFO ({@link jcsp.util.InfiniteBuffer <i>here</i>}).
 * </p>
 * <p>
 * Channels can be made {@link Poisonable} with user-chosen <i>immunity</i> (for
 * the simple and safe shutdown of networks or sub-networks).
 * </p>
 * <p>
 * Channels are either <i>one-one</i> (connecting a single writer process with a
 * single reader), <i>one-any</i> (connecting a single writer process with any
 * number of readers), <i>any-one</i> (connecting any number of writer processes
 * with a single reader) or <i>any-any</i> (connecting any number of writer
 * processes with any number of readers). Do not misuse them (e.g. use a
 * <i>one-one</i> to connect more than one writer process to more than one
 * reader).
 * </p>
 * <p>
 * Channels are used to construct process networks. Channel <i>ends</i>,
 * obtained from a channel via its <tt>in()</tt> and <tt>out()</tt> methods,
 * should be plugged into the processes that need them. An <i>input-end</i> is
 * used for reading from the channel; an <i>output-end</i> is used for writing
 * to the channel. A process should not be given a whole channel &ndash; only
 * the end that it needs.
 * </p>
 * <p>
 * Channel <i>input-ends</i> of <i>one-one</i> and <i>any-one</i> channels may
 * be used as {@link Guard <i>guards</i>} in a {@link Alternative
 * <i>choice</i>}. Channel <i>input-ends</i> of <i>one-any</i> or <i>any-any</i>
 * channels may not be so used.
 * </p>
 * <p>
 * Channel <i>output-ends</i> of <i>one-one</i> {@link One2OneChannelSymmetric
 * <i>symmetric</i>} channels may also be used as {@link Guard <i>guards</i>} in
 * a {@link Alternative <i>choice</i>}. Channel <i>output-ends</i> of all other
 * kinds of channel may not. Symmetric channels are currently an experiment:
 * buffering and poisoning are not yet supported.
 * </p>
 * <p>
 * For convenience, there are also methods for constructing arrays of channels
 * (and for extracting arrays of <i>channel-ends</i> from arrays of channels).
 * </p>
 *
 *
 * @author P.H. Welch
 */
public class Channel {

    /**
     * Private constructor to stop users from instantiating this class.
     */
    private Channel() {
        // this class should not be instantiated
    }

    /**
     * The factory to be used by this class. The class should implement
     * ChannelFactory, ChannelArrayFactory, BufferedChannelFactory and
     * BufferedChannelArrayFactory.
     */
    private static final StandardChannelFactory factory = new StandardChannelFactory();

    /* New channel construction methods ... */

    /**
     * This constructs an <i>Object carrying</i> channel that may only be connected
     * to <i>one</i> writer and <i>one</i> reader process at a time. The channel is
     * zero-buffered &ndash; the writer and reader processes must synchronise.
     *
     * @return the channel.
     */
    public static One2OneChannel one2one() {
        return new One2OneChannelImpl();
    }

    /**
     * This constructs an <i>Object carrying</i> channel that may only be connected
     * to <i>one</i> writer at a time, but <i>any</i> number of reader processes.
     * The readers contend safely with each other to take the next message. Each
     * message flows from the writer to <i>just one</i> of the readers &ndash; this
     * is not a broadcasting channel. The channel is zero-buffered &ndash; the
     * writer and reader processes must synchronise.
     *
     * @return the channel.
     */
    public static One2AnyChannel one2any() {
        return new One2AnyChannelImpl();
    }

    /**
     * This constructs an <i>Object carrying</i> channel that may be connected to
     * <i>any</i> number of writer processes, but only <i>one</i> reader at a time.
     * The writers contend safely with each other to send the next message. Each
     * message flows from <i>just one</i> of the writers to the reader &ndash; this
     * is not a combining channel. The channel is zero-buffered &ndash; the writer
     * and reader processes must synchronise.
     *
     * @return the channel.
     */
    public static Any2OneChannel any2one() {
        return new Any2OneChannelImpl();
    }

    /**
     * This constructs an <i>Object carrying</i> channel that may be connected to
     * <i>any</i> number of writer processes and <i>any</i> number of reader
     * processes. The writers contend safely with each other to send the next
     * message. The readers contend safely with each other to take the next message.
     * Each message flows from <i>just one</i> of the writers to <i>just one</i> of
     * the readers &ndash; this is not a broadcasting-and-combining channel. The
     * channel is zero-buffered &ndash; the writer and reader processes must
     * synchronise.
     *
     * @return the channel.
     */
    public static Any2AnyChannel any2any() {
        return new Any2AnyChannelImpl();
    }

    /**
     * This constructs a <i>one-one</i> Object channel with user chosen buffering
     * size and policy.
     *
     * @param buffer defines size and policy (the channel will clone its own).
     * @return the channel.
     */
    public static One2OneChannel one2one(ChannelDataStore buffer) {
        return new BufferedOne2OneChannel(buffer);
    }

    /**
     * This constructs a <i>one-any</i> Object channel with user chosen buffering
     * size and policy.
     *
     * @param buffer defines size and policy (the channel will clone its own).
     * @return the channel.
     */
    public static One2AnyChannel one2any(ChannelDataStore buffer) {
        return new BufferedOne2AnyChannel(buffer);
    }

    /**
     * This constructs an <i>any-one</i> Object channel with user chosen buffering
     * size and policy.
     *
     * @param buffer defines size and policy (the channel will clone its own).
     * @return the channel.
     */
    public static Any2OneChannel any2one(ChannelDataStore buffer) {
        return new BufferedAny2OneChannel(buffer);
    }

    /**
     * This constructs an <i>any-any</i> Object channel with user chosen buffering
     * size and policy.
     *
     * @param buffer defines size and policy (the channel will clone its own).
     * @return the channel.
     */
    public static Any2AnyChannel any2any(ChannelDataStore buffer) {
        return new BufferedAny2AnyChannel(buffer);
    }

    /**
     * This constructs a poisonable <i>one-one</i> Object channel.
     *
     * @param immunity the channel is immune to poison strengths up to and including
     *                 this level.
     * @return the channel.
     */
    public static One2OneChannel one2one(int immunity) {
        return new PoisonableOne2OneChannelImpl(immunity);
    }

    /**
     * This constructs a poisonable <i>one-any</i> Object channel.
     *
     * @param immunity the channel is immune to poison strengths up to and including
     *                 this level.
     * @return the channel.
     */
    public static One2AnyChannel one2any(int immunity) {
        return new PoisonableOne2AnyChannelImpl(immunity);
    }

    /**
     * This constructs a poisonable <i>any-one</i> Object channel.
     *
     * @param immunity the channel is immune to poison strengths up to and including
     *                 this level.
     * @return the channel.
     */
    public static Any2OneChannel any2one(int immunity) {
        return new PoisonableAny2OneChannelImpl(immunity);
    }

    /**
     * This constructs a poisonable <i>any-any</i> Object channel.
     *
     * @param immunity the channel is immune to poison strengths up to and including
     *                 this level.
     * @return the channel.
     */
    public static Any2AnyChannel any2any(int immunity) {
        return new PoisonableAny2AnyChannelImpl(immunity);
    }

    /**
     * This constructs a buffered poisonable <i>one-one</i> Object channel.
     *
     * @param buffer   defines size and policy (the channel will clone its own).
     * @param immunity the channel is immune to poison strengths up to and including
     *                 this level.
     * @return the channel.
     */
    public static One2OneChannel one2one(ChannelDataStore buffer, int immunity) {
        return new PoisonableBufferedOne2OneChannel(buffer, immunity);
    }

    /**
     * This constructs a buffered poisonable <i>one-any</i> Object channel.
     *
     * @param buffer   defines size and policy (the channel will clone its own).
     * @param immunity the channel is immune to poison strengths up to and including
     *                 this level.
     * @return the channel.
     */
    public static One2AnyChannel one2any(ChannelDataStore buffer, int immunity) {
        return new PoisonableBufferedOne2AnyChannel(buffer, immunity);
    }

    /**
     * This constructs a buffered poisonable <i>any-one</i> Object channel.
     *
     * @param buffer   defines size and policy (the channel will clone its own).
     * @param immunity the channel is immune to poison strengths up to and including
     *                 this level.
     * @return the channel.
     */
    public static Any2OneChannel any2one(ChannelDataStore buffer, int immunity) {
        return new PoisonableBufferedAny2OneChannel(buffer, immunity);
    }

    /**
     * This constructs a buffered poisonable <i>any-any</i> Object channel.
     *
     * @param buffer   defines size and policy (the channel will clone its own).
     * @param immunity the channel is immune to poison strengths up to and including
     *                 this level.
     * @return the channel.
     */
    public static Any2AnyChannel any2any(ChannelDataStore buffer, int immunity) {
        return new PoisonableBufferedAny2AnyChannel(buffer, immunity);
    }

    /**
     * This constructs an array of <i>one-one</i> Object channels.
     *
     * @param size defines size of the array (must be positive).
     * @return the channel array.
     */
    public static One2OneChannel[] one2oneArray(int size) {
        One2OneChannel[] r = new One2OneChannel[size];
        for (int i = 0; i < size; i++) {
            r[i] = one2one();
        }
        return r;
    }

    /**
     * This constructs an array of <i>one-any</i> Object channels.
     *
     * @param size defines size of the array (must be positive).
     * @return the channel array.
     */
    public static One2AnyChannel[] one2anyArray(int size) {
        One2AnyChannel[] r = new One2AnyChannel[size];
        for (int i = 0; i < size; i++) {
            r[i] = one2any();
        }
        return r;
    }

    /**
     * This constructs an array of <i>any-one</i> Object channels.
     *
     * @param size defines size of the array (must be positive).
     * @return the channel array.
     */
    public static Any2OneChannel[] any2oneArray(int size) {
        Any2OneChannel[] r = new Any2OneChannel[size];
        for (int i = 0; i < size; i++) {
            r[i] = any2one();
        }
        return r;
    }

    /**
     * This constructs an array of <i>any-any</i> Object channels.
     *
     * @param size defines size of the array (must be positive).
     * @return the channel array.
     */
    public static Any2AnyChannel[] any2anyArray(int size) {
        Any2AnyChannel[] r = new Any2AnyChannel[size];
        for (int i = 0; i < size; i++) {
            r[i] = any2any();
        }
        return r;
    }

    /**
     * This constructs an array of poisonable <i>one-one</i> Object channels.
     *
     * @param size     defines size of the array (must be positive).
     * @param immunity the channels are immune to poison strengths up to and
     *                 including this level.
     * @return the channel array.
     */
    public static One2OneChannel[] one2oneArray(int size, int immunity) {
        One2OneChannel[] r = new One2OneChannel[size];
        for (int i = 0; i < size; i++) {
            r[i] = one2one(immunity);
        }
        return r;
    }

    /**
     * This constructs an array of poisonable <i>one-any</i> Object channels.
     *
     * @param size     defines size of the array (must be positive).
     * @param immunity the channels are immune to poison strengths up to and
     *                 including this level.
     * @return the channel array.
     */
    public static One2AnyChannel[] one2anyArray(int size, int immunity) {
        One2AnyChannel[] r = new One2AnyChannel[size];
        for (int i = 0; i < size; i++) {
            r[i] = one2any(immunity);
        }
        return r;
    }

    /**
     * This constructs an array of poisonable <i>any-one</i> Object channels.
     *
     * @param size     defines size of the array (must be positive).
     * @param immunity the channels are immune to poison strengths up to and
     *                 including this level.
     * @return the channel array.
     */
    public static Any2OneChannel[] any2oneArray(int size, int immunity) {
        Any2OneChannel[] r = new Any2OneChannel[size];
        for (int i = 0; i < size; i++) {
            r[i] = any2one(immunity);
        }
        return r;
    }

    /**
     * This constructs an array of poisonable <i>any-any</i> Object channels.
     *
     * @param size     defines size of the array (must be positive).
     * @param immunity the channels are immune to poison strengths up to and
     *                 including this level.
     * @return the channel array.
     */
    public static Any2AnyChannel[] any2anyArray(int size, int immunity) {
        Any2AnyChannel[] r = new Any2AnyChannel[size];
        for (int i = 0; i < size; i++) {
            r[i] = any2any(immunity);
        }
        return r;
    }

    /**
     * This constructs an array of buffered <i>one-one</i> Object channels.
     *
     * @param size   defines size of the array (must be positive).
     * @param buffer defines size and policy (the channel will clone its own).
     * @return the channel array.
     */
    public static One2OneChannel[] one2oneArray(int size, ChannelDataStore buffer) {
        One2OneChannel[] r = new One2OneChannel[size];
        for (int i = 0; i < size; i++) {
            r[i] = one2one(buffer);
        }
        return r;
    }

    /**
     * This constructs an array of buffered <i>one-any</i> Object channels.
     *
     * @param size   defines size of the array (must be positive).
     * @param buffer defines size and policy (the channel will clone its own).
     * @return the channel array.
     */
    public static One2AnyChannel[] one2anyArray(int size, ChannelDataStore buffer) {
        One2AnyChannel[] r = new One2AnyChannel[size];
        for (int i = 0; i < size; i++) {
            r[i] = one2any(buffer);
        }
        return r;
    }

    /**
     * This constructs an array of buffered <i>any-one</i> Object channels.
     *
     * @param size   defines size of the array (must be positive).
     * @param buffer defines size and policy (the channel will clone its own).
     * @return the channel array.
     */
    public static Any2OneChannel[] any2oneArray(int size, ChannelDataStore buffer) {
        Any2OneChannel[] r = new Any2OneChannel[size];
        for (int i = 0; i < size; i++) {
            r[i] = any2one(buffer);
        }
        return r;
    }

    /**
     * This constructs an array of buffered <i>any-any</i> Object channels.
     *
     * @param size   defines size of the array (must be positive).
     * @param buffer defines size and policy (the channel will clone its own).
     * @return the channel array.
     */
    public static Any2AnyChannel[] any2anyArray(int size, ChannelDataStore buffer) {
        Any2AnyChannel[] r = new Any2AnyChannel[size];
        for (int i = 0; i < size; i++) {
            r[i] = any2any(buffer);
        }
        return r;
    }

    /**
     * This constructs an array of buffered poisonable <i>one-one</i> Object
     * channels.
     *
     * @param size     defines size of the array (must be positive).
     * @param buffer   defines size and policy (the channel will clone its own).
     * @param immunity the channels are immune to poison strengths up to and
     *                 including this level.
     * @return the channel array.
     */
    public static One2OneChannel[] one2oneArray(int size, ChannelDataStore buffer, int immunity) {
        One2OneChannel[] r = new One2OneChannel[size];
        for (int i = 0; i < size; i++) {
            r[i] = one2one(buffer, immunity);
        }
        return r;
    }

    /**
     * This constructs an array of buffered poisonable <i>one-any</i> Object
     * channels.
     *
     * @param size     defines size of the array (must be positive).
     * @param buffer   defines size and policy (the channel will clone its own).
     * @param immunity the channels are immune to poison strengths up to and
     *                 including this level.
     * @return the channel array.
     */
    public static One2AnyChannel[] one2anyArray(int size, ChannelDataStore buffer, int immunity) {
        One2AnyChannel[] r = new One2AnyChannel[size];
        for (int i = 0; i < size; i++) {
            r[i] = one2any(buffer, immunity);
        }
        return r;
    }

    /**
     * This constructs an array of buffered poisonable <i>any-one</i> Object
     * channels.
     *
     * @param size     defines size of the array (must be positive).
     * @param buffer   defines size and policy (the channel will clone its own).
     * @param immunity the channels are immune to poison strengths up to and
     *                 including this level.
     * @return the channel array.
     */
    public static Any2OneChannel[] any2oneArray(int size, ChannelDataStore buffer, int immunity) {
        Any2OneChannel[] r = new Any2OneChannel[size];
        for (int i = 0; i < size; i++) {
            r[i] = any2one(buffer, immunity);
        }
        return r;
    }

    /**
     * This constructs an array of buffered poisonable <i>any-any</i> Object
     * channels.
     *
     * @param size     defines size of the array (must be positive).
     * @param buffer   defines size and policy (the channel will clone its own).
     * @param immunity the channels are immune to poison strengths up to and
     *                 including this level.
     * @return the channel array.
     */
    public static Any2AnyChannel[] any2anyArray(int size, ChannelDataStore buffer, int immunity) {
        Any2AnyChannel[] r = new Any2AnyChannel[size];
        for (int i = 0; i < size; i++) {
            r[i] = any2any(buffer, immunity);
        }
        return r;
    }

    /**
     * This constructs an <i>Object carrying</i> channel that may only be connected
     * to <i>one</i> writer and <i>one</i> reader process at a time. The channel is
     * zero-buffered &ndash; the writer and reader processes must synchronise.
     * <p>
     * The <i>symmetry</i> relates to the use of the channel's ends as {@link Guard
     * <i>guards</i>} in an {@link Alternative}: both ends may be so used.
     *
     * @return the channel.
     */
    public static One2OneChannelSymmetric one2oneSymmetric() {
        return new BasicOne2OneChannelSymmetric();
    }

    /**
     * This constructs an array of symmetric <i>one-one</i> Object channels.
     *
     * @param size defines size of the array (must be positive).
     * @return the channel array.
     */
    public static One2OneChannelSymmetric[] one2oneSymmetricArray(int size) {
        One2OneChannelSymmetric[] r = new One2OneChannelSymmetric[size];
        for (int i = 0; i < size; i++) {
            r[i] = one2oneSymmetric();
        }
        return r;
    }

    /**
     * This constructs an <i>integer carrying</i> channel that may only be connected
     * to <i>one</i> writer and <i>one</i> reader process at a time. The channel is
     * zero-buffered &ndash; the writer and reader processes must synchronise.
     *
     * @return the channel.
     */
    public static One2OneChannelInt one2oneInt() {
        return new One2OneChannelIntImpl();
    }

    /**
     * This constructs an <i>integer carrying</i> channel that may only be connected
     * to <i>one</i> writer at a time, but <i>any</i> number of reader processes.
     * The readers contend safely with each other to take the next message. Each
     * message flows from the writer to <i>just one</i> of the readers &ndash; this
     * is not a broadcasting channel. The channel is zero-buffered &ndash; the
     * writer and reader processes must synchronise.
     *
     * @return the channel.
     */
    public static One2AnyChannelInt one2anyInt() {
        return new One2AnyChannelIntImpl();
    }

    /**
     * This constructs an <i>integer carrying</i> channel that may be connected to
     * <i>any</i> number of writer processes, but only <i>one</i> reader at a time.
     * The writers contend safely with each other to send the next message. Each
     * message flows from <i>just one</i> of the writers to the reader &ndash; this
     * is not a combining channel. The channel is zero-buffered &ndash; the writer
     * and reader processes must synchronise.
     *
     * @return the channel.
     */
    public static Any2OneChannelInt any2oneInt() {
        return new Any2OneChannelIntImpl();
    }

    /**
     * This constructs an <i>integer carrying</i> channel that may be connected to
     * <i>any</i> number of writer processes and <i>any</i> number of reader
     * processes. The writers contend safely with each other to send the next
     * message. The readers contend safely with each other to take the next message.
     * Each message flows from <i>just one</i> of the writers to <i>just one</i> of
     * the readers &ndash; this is not a broadcasting-and-combining channel. The
     * channel is zero-buffered &ndash; the writer and reader processes must
     * synchronise.
     *
     * @return the channel.
     */
    public static Any2AnyChannelInt any2anyInt() {
        return new Any2AnyChannelIntImpl();
    }

    /**
     * This constructs a <i>one-one</i> integer channel with user chosen buffering
     * size and policy.
     *
     * @param buffer defines size and policy (the channel will clone its own).
     * @return the channel.
     */
    public static One2OneChannelInt one2oneInt(ChannelDataStoreInt buffer) {
        return new BufferedOne2OneChannelIntImpl(buffer);
    }

    /**
     * This constructs a <i>one-any</i> integer channel with user chosen buffering
     * size and policy.
     *
     * @param buffer defines size and policy (the channel will clone its own).
     * @return the channel.
     */
    public static One2AnyChannelInt one2anyInt(ChannelDataStoreInt buffer) {
        return new BufferedOne2AnyChannelIntImpl(buffer);
    }

    /**
     * This constructs an <i>any-one</i> integer channel with user chosen buffering
     * size and policy.
     *
     * @param buffer defines size and policy (the channel will clone its own).
     * @return the channel.
     */
    public static Any2OneChannelInt any2oneInt(ChannelDataStoreInt buffer) {
        return new BufferedAny2OneChannelIntImpl(buffer);
    }

    /**
     * This constructs an <i>any-any</i> integer channel with user chosen buffering
     * size and policy.
     *
     * @param buffer defines size and policy (the channel will clone its own).
     * @return the channel.
     */
    public static Any2AnyChannelInt any2anyInt(ChannelDataStoreInt buffer) {
        return new BufferedAny2AnyChannelIntImpl(buffer);
    }

    /**
     * This constructs a poisonable <i>one-one</i> integer channel.
     *
     * @param immunity the channel is immune to poison strengths up to and including
     *                 this level.
     * @return the channel.
     */
    public static One2OneChannelInt one2oneInt(int immunity) {
        return new PoisonableOne2OneChannelIntImpl(immunity);
    }

    /**
     * This constructs a poisonable <i>one-any</i> integer channel.
     *
     * @param immunity the channel is immune to poison strengths up to and including
     *                 this level.
     * @return the channel.
     */
    public static One2AnyChannelInt one2anyInt(int immunity) {
        return new PoisonableOne2AnyChannelIntImpl(immunity);
    }

    /**
     * This constructs a poisonable <i>any-one</i> integer channel.
     *
     * @param immunity the channel is immune to poison strengths up to and including
     *                 this level.
     * @return the channel.
     */
    public static Any2OneChannelInt any2oneInt(int immunity) {
        return new PoisonableAny2OneChannelIntImpl(immunity);
    }

    /**
     * This constructs a poisonable <i>any-any</i> integer channel.
     *
     * @param immunity the channel is immune to poison strengths up to and including
     *                 this level.
     * @return the channel.
     */
    public static Any2AnyChannelInt any2anyInt(int immunity) {
        return new PoisonableAny2AnyChannelIntImpl(immunity);
    }

    /**
     * This constructs a buffered poisonable <i>one-one</i> integer channel.
     *
     * @param buffer   defines size and policy (the channel will clone its own).
     * @param immunity the channel is immune to poison strengths up to and including
     *                 this level.
     * @return the channel.
     */
    public static One2OneChannelInt one2oneInt(ChannelDataStoreInt buffer, int immunity) {
        return new PoisonableBufferedOne2OneChannelInt(buffer, immunity);
    }

    /**
     * This constructs a buffered poisonable <i>one-any</i> integer channel.
     *
     * @param buffer   defines size and policy (the channel will clone its own).
     * @param immunity the channel is immune to poison strengths up to and including
     *                 this level.
     * @return the channel.
     */
    public static One2AnyChannelInt one2anyInt(ChannelDataStoreInt buffer, int immunity) {
        return new PoisonableBufferedOne2AnyChannelInt(buffer, immunity);
    }

    /**
     * This constructs a buffered poisonable <i>any-one</i> integer channel.
     *
     * @param buffer   defines size and policy (the channel will clone its own).
     * @param immunity the channel is immune to poison strengths up to and including
     *                 this level.
     * @return the channel.
     */
    public static Any2OneChannelInt any2oneInt(ChannelDataStoreInt buffer, int immunity) {
        return new PoisonableBufferedAny2OneChannelInt(buffer, immunity);
    }

    /**
     * This constructs a buffered poisonable <i>any-any</i> integer channel.
     *
     * @param buffer   defines size and policy (the channel will clone its own).
     * @param immunity the channel is immune to poison strengths up to and including
     *                 this level.
     * @return the channel.
     */
    public static Any2AnyChannelInt any2anyInt(ChannelDataStoreInt buffer, int immunity) {
        return new PoisonableBufferedAny2AnyChannelInt(buffer, immunity);
    }

    /**
     * This constructs an array of <i>one-one</i> integer channels.
     *
     * @param size defines size of the array (must be positive).
     * @return the channel array.
     */
    public static One2OneChannelInt[] one2oneIntArray(int size) {
        One2OneChannelInt[] r = new One2OneChannelInt[size];
        for (int i = 0; i < size; i++) {
            r[i] = one2oneInt();
        }
        return r;
    }

    /**
     * This constructs an array of <i>one-any</i> integer channels.
     *
     * @param size defines size of the array (must be positive).
     * @return the channel array.
     */
    public static One2AnyChannelInt[] one2anyIntArray(int size) {
        One2AnyChannelInt[] r = new One2AnyChannelInt[size];
        for (int i = 0; i < size; i++) {
            r[i] = one2anyInt();
        }
        return r;
    }

    /**
     * This constructs an array of <i>any-one</i> integer channels.
     *
     * @param size defines size of the array (must be positive).
     * @return the channel array.
     */
    public static Any2OneChannelInt[] any2oneIntArray(int size) {
        Any2OneChannelInt[] r = new Any2OneChannelInt[size];
        for (int i = 0; i < size; i++) {
            r[i] = any2oneInt();
        }
        return r;
    }

    /**
     * This constructs an array of <i>any-any</i> integer channels.
     *
     * @param size defines size of the array (must be positive).
     * @return the channel array.
     */
    public static Any2AnyChannelInt[] any2anyIntArray(int size) {
        Any2AnyChannelInt[] r = new Any2AnyChannelInt[size];
        for (int i = 0; i < size; i++) {
            r[i] = any2anyInt();
        }
        return r;
    }

    /**
     * This constructs an array of poisonable <i>one-one</i> integer channels.
     *
     * @param size     defines size of the array (must be positive).
     * @param immunity the channels are immune to poison strengths up to and
     *                 including this level.
     * @return the channel array.
     */
    public static One2OneChannelInt[] one2oneIntArray(int size, int immunity) {
        One2OneChannelInt[] r = new One2OneChannelInt[size];
        for (int i = 0; i < size; i++) {
            r[i] = one2oneInt(immunity);
        }
        return r;
    }

    /**
     * This constructs an array of poisonable <i>one-any</i> integer channels.
     *
     * @param size     defines size of the array (must be positive).
     * @param immunity the channels are immune to poison strengths up to and
     *                 including this level.
     * @return the channel array.
     */
    public static One2AnyChannelInt[] one2anyIntArray(int size, int immunity) {
        One2AnyChannelInt[] r = new One2AnyChannelInt[size];
        for (int i = 0; i < size; i++) {
            r[i] = one2anyInt(immunity);
        }
        return r;
    }

    /**
     * This constructs an array of poisonable <i>any-one</i> integer channels.
     *
     * @param size     defines size of the array (must be positive).
     * @param immunity the channels are immune to poison strengths up to and
     *                 including this level.
     * @return the channel array.
     */
    public static Any2OneChannelInt[] any2oneIntArray(int size, int immunity) {
        Any2OneChannelInt[] r = new Any2OneChannelInt[size];
        for (int i = 0; i < size; i++) {
            r[i] = any2oneInt(immunity);
        }
        return r;
    }

    /**
     * This constructs an array of poisonable <i>any-any</i> integer channels.
     *
     * @param size     defines size of the array (must be positive).
     * @param immunity the channels are immune to poison strengths up to and
     *                 including this level.
     * @return the channel array.
     */
    public static Any2AnyChannelInt[] any2anyIntArray(int size, int immunity) {
        Any2AnyChannelInt[] r = new Any2AnyChannelInt[size];
        for (int i = 0; i < size; i++) {
            r[i] = any2anyInt(immunity);
        }
        return r;
    }

    /**
     * This constructs an array of buffered <i>one-one</i> integer channels.
     *
     * @param size   defines size of the array (must be positive).
     * @param buffer defines size and policy (the channel will clone its own).
     * @return the channel array.
     */
    public static One2OneChannelInt[] one2oneIntArray(int size, ChannelDataStoreInt buffer) {
        One2OneChannelInt[] r = new One2OneChannelInt[size];
        for (int i = 0; i < size; i++) {
            r[i] = one2oneInt(buffer);
        }
        return r;
    }

    /**
     * This constructs an array of buffered <i>one-any</i> integer channels.
     *
     * @param size   defines size of the array (must be positive).
     * @param buffer defines size and policy (the channel will clone its own).
     * @return the channel array.
     */
    public static One2AnyChannelInt[] one2anyIntArray(int size, ChannelDataStoreInt buffer) {
        One2AnyChannelInt[] r = new One2AnyChannelInt[size];
        for (int i = 0; i < size; i++) {
            r[i] = one2anyInt(buffer);
        }
        return r;
    }

    /**
     * This constructs an array of buffered <i>any-one</i> integer channels.
     *
     * @param size   defines size of the array (must be positive).
     * @param buffer defines size and policy (the channel will clone its own).
     * @return the channel array.
     */
    public static Any2OneChannelInt[] any2oneIntArray(int size, ChannelDataStoreInt buffer) {
        Any2OneChannelInt[] r = new Any2OneChannelInt[size];
        for (int i = 0; i < size; i++) {
            r[i] = any2oneInt(buffer);
        }
        return r;
    }

    /**
     * This constructs an array of buffered <i>any-any</i> integer channels.
     *
     * @param size   defines size of the array (must be positive).
     * @param buffer defines size and policy (the channel will clone its own).
     * @return the channel array.
     */
    public static Any2AnyChannelInt[] any2anyIntArray(int size, ChannelDataStoreInt buffer) {
        Any2AnyChannelInt[] r = new Any2AnyChannelInt[size];
        for (int i = 0; i < size; i++) {
            r[i] = any2anyInt(buffer);
        }
        return r;
    }

    /**
     * This constructs an array of buffered poisonable <i>one-one</i> integer
     * channels.
     *
     * @param size     defines size of the array (must be positive).
     * @param buffer   defines size and policy (the channel will clone its own).
     * @param immunity the channels are immune to poison strengths up to and
     *                 including this level.
     * @return the channel array.
     */
    public static One2OneChannelInt[] one2oneIntArray(int size, ChannelDataStoreInt buffer, int immunity) {
        One2OneChannelInt[] r = new One2OneChannelInt[size];
        for (int i = 0; i < size; i++) {
            r[i] = one2oneInt(buffer, immunity);
        }
        return r;
    }

    /**
     * This constructs an array of buffered poisonable <i>one-any</i> integer
     * channels.
     *
     * @param size     defines size of the array (must be positive).
     * @param buffer   defines size and policy (the channel will clone its own).
     * @param immunity the channels are immune to poison strengths up to and
     *                 including this level.
     * @return the channel array.
     */
    public static One2AnyChannelInt[] one2anyIntArray(int size, ChannelDataStoreInt buffer, int immunity) {
        One2AnyChannelInt[] r = new One2AnyChannelInt[size];
        for (int i = 0; i < size; i++) {
            r[i] = one2anyInt(buffer, immunity);
        }
        return r;
    }

    /**
     * This constructs an array of buffered poisonable <i>any-one</i> integer
     * channels.
     *
     * @param size     defines size of the array (must be positive).
     * @param buffer   defines size and policy (the channel will clone its own).
     * @param immunity the channels are immune to poison strengths up to and
     *                 including this level.
     * @return the channel array.
     */
    public static Any2OneChannelInt[] any2oneIntArray(int size, ChannelDataStoreInt buffer, int immunity) {
        Any2OneChannelInt[] r = new Any2OneChannelInt[size];
        for (int i = 0; i < size; i++) {
            r[i] = any2oneInt(buffer, immunity);
        }
        return r;
    }

    /**
     * This constructs an array of buffered poisonable <i>any-any</i> integer
     * channels.
     *
     * @param size     defines size of the array (must be positive).
     * @param buffer   defines size and policy (the channel will clone its own).
     * @param immunity the channels are immune to poison strengths up to and
     *                 including this level.
     * @return the channel array.
     */
    public static Any2AnyChannelInt[] any2anyIntArray(int size, ChannelDataStoreInt buffer, int immunity) {
        Any2AnyChannelInt[] r = new Any2AnyChannelInt[size];
        for (int i = 0; i < size; i++) {
            r[i] = any2anyInt(buffer, immunity);
        }
        return r;
    }

    /**
     * This constructs an <i>integer carrying</i> channel that may only be connected
     * to <i>one</i> writer and <i>one</i> reader process at a time. The channel is
     * zero-buffered &ndash; the writer and reader processes must synchronise.
     * <p>
     * The <i>symmetry</i> relates to the use of the channel's ends as {@link Guard
     * <i>guards</i>} in an {@link Alternative}: both ends may be so used.
     *
     * @return the channel.
     */
    public static One2OneChannelSymmetricInt one2oneSymmetricInt() {
        return new BasicOne2OneChannelSymmetricInt();
    }

    /**
     * This constructs an array of symmetric <i>one-one</i> integer channels.
     *
     * @param size defines size of the array (must be positive).
     * @return the channel array.
     */
    public static One2OneChannelSymmetricInt[] one2oneSymmetricIntArray(int size) {
        One2OneChannelSymmetricInt[] r = new One2OneChannelSymmetricInt[size];
        for (int i = 0; i < size; i++) {
            r[i] = one2oneSymmetricInt();
        }
        return r;
    }

    /* Helper methods to get arrays of channel ends ... */

    /**
     * This extracts the <i>input-ends</i> from the given channel array. Each
     * element of the returned array is the <i>input-end</i> of the channel at the
     * corresponding index in the given channel array.
     *
     * @param c an array of channels.
     * @return the array of <i>input-ends</i> from the given channel array.
     */
    public static AltingChannelInput[] getInputArray(One2OneChannel[] c) {
        AltingChannelInput[] in = new AltingChannelInput[c.length];
        for (int i = 0; i < c.length; i++)
            in[i] = c[i].in();
        return in;
    }

    /**
     * This extracts the <i>input-ends</i> from the given channel array. Each
     * element of the returned array is the <i>input-end</i> of the channel at the
     * corresponding index in the given channel array.
     *
     * @param c an array of channels.
     * @return the array of <i>input-ends</i> from the given channel array.
     */
    public static SharedChannelInput[] getInputArray(One2AnyChannel[] c) {
        SharedChannelInput[] in = new SharedChannelInput[c.length];
        for (int i = 0; i < c.length; i++)
            in[i] = c[i].in();
        return in;
    }

    /**
     * This extracts the <i>input-ends</i> from the given channel array. Each
     * element of the returned array is the <i>input-end</i> of the channel at the
     * corresponding index in the given channel array.
     *
     * @param c an array of channels.
     * @return the array of <i>input-ends</i> from the given channel array.
     */
    public static AltingChannelInput[] getInputArray(Any2OneChannel[] c) {
        AltingChannelInput[] in = new AltingChannelInput[c.length];
        for (int i = 0; i < c.length; i++)
            in[i] = c[i].in();
        return in;
    }

    /**
     * This extracts the <i>input-ends</i> from the given channel array. Each
     * element of the returned array is the <i>input-end</i> of the channel at the
     * corresponding index in the given channel array.
     *
     * @param c an array of channels.
     * @return the array of <i>input-ends</i> from the given channel array.
     */
    public static SharedChannelInput[] getInputArray(Any2AnyChannel[] c) {
        SharedChannelInput[] in = new SharedChannelInput[c.length];
        for (int i = 0; i < c.length; i++)
            in[i] = c[i].in();
        return in;
    }

    /**
     * This extracts the <i>output-ends</i> from the given channel array. Each
     * element of the returned array is the <i>output-end</i> of the channel at the
     * corresponding index in the given channel array.
     *
     * @param c an array of channels.
     * @return the array of <i>output-ends</i> from the given channel array.
     */
    public static ChannelOutput[] getOutputArray(One2OneChannel[] c) {
        ChannelOutput[] in = new ChannelOutput[c.length];
        for (int i = 0; i < c.length; i++)
            in[i] = c[i].out();
        return in;
    }

    /**
     * This extracts the <i>output-ends</i> from the given channel array. Each
     * element of the returned array is the <i>output-end</i> of the channel at the
     * corresponding index in the given channel array.
     *
     * @param c an array of channels.
     * @return the array of <i>output-ends</i> from the given channel array.
     */
    public static ChannelOutput[] getOutputArray(One2AnyChannel[] c) {
        ChannelOutput[] in = new ChannelOutput[c.length];
        for (int i = 0; i < c.length; i++)
            in[i] = c[i].out();
        return in;
    }

    /**
     * This extracts the <i>output-ends</i> from the given channel array. Each
     * element of the returned array is the <i>output-end</i> of the channel at the
     * corresponding index in the given channel array.
     *
     * @param c an array of channels.
     * @return the array of <i>output-ends</i> from the given channel array.
     */
    public static SharedChannelOutput[] getOutputArray(Any2OneChannel[] c) {
        SharedChannelOutput[] in = new SharedChannelOutput[c.length];
        for (int i = 0; i < c.length; i++)
            in[i] = c[i].out();
        return in;
    }

    /**
     * This extracts the <i>output-ends</i> from the given channel array. Each
     * element of the returned array is the <i>output-end</i> of the channel at the
     * corresponding index in the given channel array.
     *
     * @param c an array of channels.
     * @return the array of <i>output-ends</i> from the given channel array.
     */
    public static SharedChannelOutput[] getOutputArray(Any2AnyChannel[] c) {
        SharedChannelOutput[] in = new SharedChannelOutput[c.length];
        for (int i = 0; i < c.length; i++)
            in[i] = c[i].out();
        return in;
    }

    /**
     * This extracts the <i>input-ends</i> from the given channel array. Each
     * element of the returned array is the <i>input-end</i> of the channel at the
     * corresponding index in the given channel array.
     *
     * @param c an array of channels.
     * @return the array of <i>input-ends</i> from the given channel array.
     */
    public static AltingChannelInputInt[] getInputArray(One2OneChannelInt[] c) {
        AltingChannelInputInt[] in = new AltingChannelInputInt[c.length];
        for (int i = 0; i < c.length; i++)
            in[i] = c[i].in();
        return in;
    }

    /**
     * This extracts the <i>input-ends</i> from the given channel array. Each
     * element of the returned array is the <i>input-end</i> of the channel at the
     * corresponding index in the given channel array.
     *
     * @param c an array of channels.
     * @return the array of <i>input-ends</i> from the given channel array.
     */
    public static SharedChannelInputInt[] getInputArray(One2AnyChannelInt[] c) {
        SharedChannelInputInt[] in = new SharedChannelInputInt[c.length];
        for (int i = 0; i < c.length; i++)
            in[i] = c[i].in();
        return in;
    }

    /**
     * This extracts the <i>input-ends</i> from the given channel array. Each
     * element of the returned array is the <i>input-end</i> of the channel at the
     * corresponding index in the given channel array.
     *
     * @param c an array of channels.
     * @return the array of <i>input-ends</i> from the given channel array.
     */
    public static AltingChannelInputInt[] getInputArray(Any2OneChannelInt[] c) {
        AltingChannelInputInt[] in = new AltingChannelInputInt[c.length];
        for (int i = 0; i < c.length; i++)
            in[i] = c[i].in();
        return in;
    }

    /**
     * This extracts the <i>input-ends</i> from the given channel array. Each
     * element of the returned array is the <i>input-end</i> of the channel at the
     * corresponding index in the given channel array.
     *
     * @param c an array of channels.
     * @return the array of <i>input-ends</i> from the given channel array.
     */
    public static SharedChannelInputInt[] getInputArray(Any2AnyChannelInt[] c) {
        SharedChannelInputInt[] in = new SharedChannelInputInt[c.length];
        for (int i = 0; i < c.length; i++)
            in[i] = c[i].in();
        return in;
    }

    /**
     * This extracts the <i>output-ends</i> from the given channel array. Each
     * element of the returned array is the <i>output-end</i> of the channel at the
     * corresponding index in the given channel array.
     *
     * @param c an array of channels.
     * @return the array of <i>output-ends</i> from the given channel array.
     */
    public static ChannelOutputInt[] getOutputArray(One2OneChannelInt[] c) {
        ChannelOutputInt[] in = new ChannelOutputInt[c.length];
        for (int i = 0; i < c.length; i++)
            in[i] = c[i].out();
        return in;
    }

    /**
     * This extracts the <i>output-ends</i> from the given channel array. Each
     * element of the returned array is the <i>output-end</i> of the channel at the
     * corresponding index in the given channel array.
     *
     * @param c an array of channels.
     * @return the array of <i>output-ends</i> from the given channel array.
     */
    public static ChannelOutputInt[] getOutputArray(One2AnyChannelInt[] c) {
        ChannelOutputInt[] in = new ChannelOutputInt[c.length];
        for (int i = 0; i < c.length; i++)
            in[i] = c[i].out();
        return in;
    }

    /**
     * This extracts the <i>output-ends</i> from the given channel array. Each
     * element of the returned array is the <i>output-end</i> of the channel at the
     * corresponding index in the given channel array.
     *
     * @param c an array of channels.
     * @return the array of <i>output-ends</i> from the given channel array.
     */
    public static SharedChannelOutputInt[] getOutputArray(Any2OneChannelInt[] c) {
        SharedChannelOutputInt[] in = new SharedChannelOutputInt[c.length];
        for (int i = 0; i < c.length; i++)
            in[i] = c[i].out();
        return in;
    }

    /**
     * This extracts the <i>output-ends</i> from the given channel array. Each
     * element of the returned array is the <i>output-end</i> of the channel at the
     * corresponding index in the given channel array.
     *
     * @param c an array of channels.
     * @return the array of <i>output-ends</i> from the given channel array.
     */
    public static SharedChannelOutputInt[] getOutputArray(Any2AnyChannelInt[] c) {
        SharedChannelOutputInt[] in = new SharedChannelOutputInt[c.length];
        for (int i = 0; i < c.length; i++)
            in[i] = c[i].out();
        return in;
    }

    /* Methods that are the same as the Factory Methods (all now deprecated) */

    /**
     * Constructs and returns a <code>One2OneChannel</code> object.
     *
     * @return the channel.
     *
     * @see ChannelFactory#createOne2One()
     * 
     * @deprecated Use the {@link #one2one()} method instead.
     */
    @Deprecated
    public static One2OneChannel createOne2One() {
        return factory.createOne2One();
    }

    /**
     * Constructs and returns an <code>Any2OneChannel</code> object.
     *
     * @return the channel.
     *
     * @see ChannelFactory#createAny2One()
     * 
     * @deprecated Use the {@link #any2one()} method instead.
     */
    @Deprecated
    public static Any2OneChannel createAny2One() {
        return factory.createAny2One();
    }

    /**
     * Constructs and returns a <code>One2AnyChannel</code> object.
     *
     * @return the channel.
     *
     * @see ChannelFactory#createOne2Any()
     * 
     * @deprecated Use the {@link #one2any()} method instead.
     */
    @Deprecated
    public static One2AnyChannel createOne2Any() {
        return factory.createOne2Any();
    }

    /**
     * Constructs and returns an <code>Any2AnyChannel</code> object.
     *
     * @return the channel.
     *
     * @see ChannelFactory#createAny2Any()
     * 
     * @deprecated Use the {@link #any2any()} method instead.
     */
    @Deprecated
    public static Any2AnyChannel createAny2Any() {
        return factory.createAny2Any();
    }

    /**
     * Constructs and returns an array of <code>One2OneChannel</code> objects.
     *
     * @param n the size of the array of channels.
     * @return the array of channels.
     *
     * @see ChannelArrayFactory#createOne2One(int)
     * 
     * @deprecated Use the {@link #one2oneArray(int)} method instead.
     */
    @Deprecated
    public static One2OneChannel[] createOne2One(int n) {
        return factory.createOne2One(n);
    }

    /**
     * Constructs and returns an array of <code>Any2OneChannel</code> objects.
     *
     * @param n the size of the array of channels.
     * @return the array of channels.
     *
     * @see ChannelArrayFactory#createAny2One(int)
     * 
     * @deprecated Use the {@link #any2oneArray(int)} method instead.
     */
    @Deprecated
    public static Any2OneChannel[] createAny2One(int n) {
        return factory.createAny2One(n);
    }

    /**
     * Constructs and returns an array of <code>One2AnyChannel</code> objects.
     *
     * @param n the size of the array of channels.
     * @return the array of channels.
     *
     * @see ChannelArrayFactory#createOne2Any(int)
     * 
     * @deprecated Use the {@link #one2anyArray(int)} method instead.
     */
    @Deprecated
    public static One2AnyChannel[] createOne2Any(int n) {
        return factory.createOne2Any(n);
    }

    /**
     * Constructs and returns an array of <code>Any2AnyChannel</code> objects.
     *
     * @param n the size of the array of channels.
     * @return the array of channels.
     *
     * @see ChannelArrayFactory#createAny2Any(int)
     * 
     * @deprecated Use the {@link #any2anyArray(int)} method instead.
     */
    @Deprecated
    public static Any2AnyChannel[] createAny2Any(int n) {
        return factory.createAny2Any(n);
    }

    /**
     * <p>
     * Constructs and returns a <code>One2OneChannel</code> object which uses the
     * specified <code>ChannelDataStore</code> object as a buffer.
     * </p>
     * <p>
     * The buffer supplied to this method is cloned before it is inserted into the
     * channel.
     * </p>
     *
     * @param buffer the <code>ChannelDataStore</code> to use.
     * @return the buffered channel.
     *
     * @see BufferedChannelFactory#createOne2One(ChannelDataStore)
     * @see ChannelDataStore
     * 
     * @deprecated Use the {@link #one2one(ChannelDataStore)} method instead.
     */
    @Deprecated
    public static One2OneChannel createOne2One(ChannelDataStore buffer) {
        return factory.createOne2One(buffer);
    }

    /**
     * <p>
     * Constructs and returns a <code>Any2OneChannel</code> object which uses the
     * specified <code>ChannelDataStore</code> object as a buffer.
     * </p>
     * <p>
     * The buffer supplied to this method is cloned before it is inserted into the
     * channel.
     * </p>
     *
     * @param buffer the <code>ChannelDataStore</code> to use.
     * @return the buffered channel.
     *
     * @see BufferedChannelFactory#createAny2One(ChannelDataStore)
     * @see ChannelDataStore
     * 
     * @deprecated Use the {@link #any2one(ChannelDataStore)} method instead.
     */
    @Deprecated
    public static Any2OneChannel createAny2One(ChannelDataStore buffer) {
        return factory.createAny2One(buffer);
    }

    /**
     * <p>
     * Constructs and returns a <code>One2AnyChannel</code> object which uses the
     * specified <code>ChannelDataStore</code> object as a buffer.
     * </p>
     * <p>
     * The buffer supplied to this method is cloned before it is inserted into the
     * channel.
     * </p>
     *
     * @param buffer the <code>ChannelDataStore</code> to use.
     * @return the buffered channel.
     *
     * @see BufferedChannelFactory#createOne2Any(ChannelDataStore)
     * @see ChannelDataStore
     * 
     * @deprecated Use the {@link #one2any(ChannelDataStore)} method instead.
     */
    @Deprecated
    public static One2AnyChannel createOne2Any(ChannelDataStore buffer) {
        return factory.createOne2Any(buffer);
    }

    /**
     * <p>
     * Constructs and returns a <code>Any2AnyChannel</code> object which uses the
     * specified <code>ChannelDataStore</code> object as a buffer.
     * </p>
     * <p>
     * The buffer supplied to this method is cloned before it is inserted into the
     * channel.
     * </p>
     *
     * @param buffer the <code>ChannelDataStore</code> to use.
     * @return the buffered channel.
     *
     * @see BufferedChannelFactory#createAny2Any(ChannelDataStore)
     * @see ChannelDataStore
     * 
     * @deprecated Use the {@link #any2any(ChannelDataStore)} method instead.
     */
    @Deprecated
    public static Any2AnyChannel createAny2Any(ChannelDataStore buffer) {
        return factory.createAny2Any(buffer);
    }

    /**
     * <p>
     * Constructs and returns an array of <code>One2OneChannel</code> objects which
     * use the specified <code>ChannelDataStore</code> object as a buffer.
     * </p>
     * <p>
     * The buffer supplied to this method is cloned before it is inserted into the
     * channel. This is why an array of buffers is not required.
     * </p>
     *
     * @param buffer the <code>ChannelDataStore</code> to use.
     * @param n      the size of the array of channels.
     * @return the array of buffered channels.
     *
     * @see BufferedChannelArrayFactory#createOne2One(ChannelDataStore, int)
     * @see ChannelDataStore
     * 
     * @deprecated Use the {@link #one2oneArray(int,ChannelDataStore)} method
     *             instead.
     */
    @Deprecated
    public static One2OneChannel[] createOne2One(ChannelDataStore buffer, int n) {
        return factory.createOne2One(buffer, n);
    }

    /**
     * <p>
     * Constructs and returns an array of <code>Any2OneChannel</code> objects which
     * use the specified <code>ChannelDataStore</code> object as a buffer.
     * </p>
     * <p>
     * The buffer supplied to this method is cloned before it is inserted into the
     * channel. This is why an array of buffers is not required.
     * </p>
     *
     * @param buffer the <code>ChannelDataStore</code> to use.
     * @param n      the size of the array of channels.
     * @return the array of buffered channels.
     *
     * @see BufferedChannelArrayFactory#createAny2One(ChannelDataStore, int)
     * @see ChannelDataStore
     * 
     * @deprecated Use the {@link #any2oneArray(int,ChannelDataStore)} method
     *             instead.
     */
    @Deprecated
    public static Any2OneChannel[] createAny2One(ChannelDataStore buffer, int n) {
        return factory.createAny2One(buffer, n);
    }

    /**
     * <p>
     * Constructs and returns an array of <code>One2AnyChannel</code> objects which
     * use the specified <code>ChannelDataStore</code> object as a buffer.
     * </p>
     * <p>
     * The buffer supplied to this method is cloned before it is inserted into the
     * channel. This is why an array of buffers is not required.
     * </p>
     *
     * @param buffer the <code>ChannelDataStore</code> to use.
     * @param n      the size of the array of channels.
     * @return the array of buffered channels.
     *
     * @see BufferedChannelArrayFactory#createOne2Any(ChannelDataStore, int)
     * @see ChannelDataStore
     * 
     * @deprecated Use the {@link #one2anyArray(int,ChannelDataStore)} method
     *             instead.
     */
    @Deprecated
    public static One2AnyChannel[] createOne2Any(ChannelDataStore buffer, int n) {
        return factory.createOne2Any(buffer, n);
    }

    /**
     * <p>
     * Constructs and returns an array of <code>Any2AnyChannel</code> objects which
     * use the specified <code>ChannelDataStore</code> object as a buffer.
     * </p>
     * <p>
     * The buffer supplied to this method is cloned before it is inserted into the
     * channel. This is why an array of buffers is not required.
     * </p>
     *
     * @param buffer the <code>ChannelDataStore</code> to use.
     * @param n      the size of the array of channels.
     * @return the array of buffered channels.
     *
     * @see BufferedChannelArrayFactory#createAny2Any(ChannelDataStore, int)
     * @see ChannelDataStore
     * 
     * @deprecated Use the {@link #any2anyArray(int,ChannelDataStore)} method
     *             instead.
     */
    @Deprecated
    public static Any2AnyChannel[] createAny2Any(ChannelDataStore buffer, int n) {
        return factory.createAny2Any(buffer, n);
    }

}
