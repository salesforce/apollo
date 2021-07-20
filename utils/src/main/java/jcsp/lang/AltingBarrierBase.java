
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

class AltingBarrierBase { // package-only visible class

    /**
     * All front-ends are chained off here. Each process enrolled must have one, and
     * only one, of these.
     */
    private AltingBarrier frontEnds = null;

    /** The number of processes enrolled. */
    private int enrolled = 0;

    /** The number of processes not yet offered to sync on this barrier. */
    private int countdown = 0;

    /*
     * This creates, and returns, more front-ends to be held by newly enrolling
     * processes. Initially, none exist - so this (or {@link #expand()}) must be
     * called at least once. <p> <i>Note: except for the first time, this method
     * should only be called by an AltingBarrier synchronised on this
     * AltingBarrierBase.</I> <p>
     * 
     * @param n the number of front-ends to be created. <p>
     *
     * @return the new front-ends.
     * 
     */
    AltingBarrier[] expand(int n) {
        AltingBarrier[] ab = new AltingBarrier[n];
        for (int i = 0; i < n; i++) {
            frontEnds = new AltingBarrier(this, frontEnds);
            ab[i] = frontEnds;
        }
        enrolled += n;
        countdown += n;
        return ab;
    }

    /*
     * This creates, and returns, another front-end to be held by a newly enrolling
     * process. Initially, none exist - so this (or {@link #expand(int)}) must be
     * called at least once. <p> <i>Note: except for the first time, this method
     * should only be called by an AltingBarrier synchronised on this
     * AltingBarrierBase.</I> <p>
     *
     * @return the new front-ends.
     * 
     */
    AltingBarrier expand() {
        enrolled++;
        countdown++;
        frontEnds = new AltingBarrier(this, frontEnds);
        return frontEnds;
    }

    /**
     * This removes the given <i>front-ends</i> chained to this <i>alting</i>
     * barrier. It also nulls all of them - to prevent any attempted reuse!
     * <p>
     * <i>Note: this method should only be called by an AltingBarrier synchronised
     * on this AltingBarrierBase.</I>
     * <p>
     *
     * @param ab the <i>front-ends</i> being discarded from this barrier. This array
     *           must be unaltered from one previously delivered by an
     *           {@link #expand expand}.
     */
    void contract(AltingBarrier[] ab) {

        // assume: (ab != null) && (ab.length > 0)
        AltingBarrier first = ab[0];

        // counts the number of front-ends whose (hopefully terminated) processes
        // were still enrolled.
        int discard = 0;

        AltingBarrier fa = null;
        AltingBarrier fb = frontEnds;
        while ((fb != null) && (fb != first)) {
            fa = fb;
            fb = fb.next;
        }

        if (fb == null) {
            throw new AltingBarrierError("\n*** Could not find first front-end in AltingBarrier contract.");
        }

        // Below, we will null elements of "ab" as we pass though the array.
        // However, the formal "deduce" and "invariant" comments that follow
        // relate to code that does not do this. This is safe since the logic
        // of the code does not depend on values of "ab", subsequent to their
        // anullment.

        ab[0].base = null;
        ab[0] = null;

        // deduce: (fb == ab[0]) && (fb != null)
        // deduce: (fa == null) || (fa.next == fb)
        // deduce: (fa == null) <==> (frontEnds == ab[0])
        // deduce: (fa != null) <==> (fa.next == ab[0])

        for (int i = 1; i < ab.length; i++) {
            // invariant: (fb == ab[i-1]) && (fb != null)
            if (fb.enrolled)
                discard++;
            fb = fb.next;
            if (fb == null) {
                throw new AltingBarrierError("\n*** Could not find second (or later) front-end in AltingBarrier contract.");
            }
            if (fb != ab[i]) {
                throw new AltingBarrierError("\n*** Removal array in AltingBarrier contract not one delivered by expand.");
            }
            // deduce: (fb == ab[i]) && (fb != null)
            ab[i].base = null;
            ab[i] = null;
        }

        // deduce: (fb == ab[(ab.length) - 1]) && (fb != null)

        if (fb.enrolled)
            discard++;

        // deduce: (fa == null) <==> (frontEnds == ab[0]) [NO CHANGE]
        // deduce: (fa != null) <==> (fa.next == ab[0]) [NO CHANGE]

        if (fa == null) {
            frontEnds = fb.next;
        } else {
            fa.next = fb.next;
        }

        enrolled -= discard;
        countdown -= discard;
        if (countdown == 0) {
            countdown = enrolled;
            if (enrolled > 0) {
                AltingBarrierCoordinate.startEnable();
                AltingBarrierCoordinate.startDisable(enrolled);
                AltingBarrier fe = frontEnds;
                while (fe != null) {
                    fe.schedule();
                    fe = fe.next;
                }
            }
        } else if (countdown < 0) {
            throw new JCSP_InternalError("Please report the circumstances to jcsp-team@kent.ac.uk - thanks!");
        }

    }

    /**
     * This removes the given <i>front-end</i> chained to this <i>alting</i>
     * barrier. It also nulls its reference to this base - to prevent any attempted
     * reuse!
     * <p>
     * <i>Note: this method should only be called by an AltingBarrier synchronised
     * on this AltingBarrierBase.</I>
     * <p>
     *
     * @param ab the <i>front-end</i> being discarded from this barrier. This array
     *           must be unaltered from one previously delivered by an
     *           {@link #expand expand}.
     */
    void contract(AltingBarrier ab) {

        // assume: (ab != null)

        AltingBarrier fa = null;
        AltingBarrier fb = frontEnds;
        while ((fb != null) && (fb != ab)) {
            fa = fb;
            fb = fb.next;
        }

        if (fb == null) {
            throw new AltingBarrierError("\n*** Could not find front-end in AltingBarrier contract.");
        }

        // deduce: (fb == ab) && (fb != null)
        // deduce: (fa == null) || (fa.next == fb)
        // deduce: (fa == null) <==> (frontEnds == ab)
        // deduce: (fa != null) <==> (fa.next == ab)

        if (fa == null) {
            frontEnds = fb.next;
        } else {
            fa.next = fb.next;
        }

        ab.base = null;

        if (ab.enrolled) {
            enrolled--;
            countdown--;
        }
        if (countdown == 0) {
            countdown = enrolled;
            if (enrolled > 0) {
                AltingBarrierCoordinate.startEnable();
                AltingBarrierCoordinate.startDisable(enrolled);
                AltingBarrier fe = frontEnds;
                while (fe != null) {
                    fe.schedule();
                    fe = fe.next;
                }
            }
        } else if (countdown < 0) {
            throw new JCSP_InternalError("Please report the circumstances to jcsp-team@kent.ac.uk - thanks!");
        }

    }

    /**
     * Record the offer to synchronise.
     * <P>
     * <I>Note: this method should only be called by an AltingBarrier synchronised
     * on this AltingBarrierBase.</I>
     * <p>
     *
     * @return true if all the offers are in.
     */
    boolean enable() {
        countdown--;
        if (countdown == 0) {
            countdown = enrolled;
            AltingBarrierCoordinate.startDisable(enrolled);
            AltingBarrier fe = frontEnds;
            while (fe != null) {
                fe.schedule();
                fe = fe.next;
            }
            return true;
        } else {
            return false;
        }
    }

    /**
     * Withdraw the offer to synchronise.
     * <P>
     * <I>Note: this method should only be called by an AltingBarrier synchronised
     * on this AltingBarrierBase.</I>
     * <p>
     *
     * @return true all the offers are in.
     */
    boolean disable() {
        if (countdown == enrolled) {
            return true;
        } else {
            countdown++;
            return false;
        }
    }

    /**
     * Record resignation.
     * <p>
     * <I>Note: this method should only be called by an AltingBarrier synchronised
     * on this AltingBarrierBase.</I>
     * <p>
     */
    void resign() {
        enrolled--;
        countdown--;
        if (countdown == 0) {
            countdown = enrolled;
            if (enrolled > 0) {
                AltingBarrierCoordinate.startEnable();
                AltingBarrierCoordinate.startDisable(enrolled);
                AltingBarrier fe = frontEnds;
                while (fe != null) {
                    fe.schedule();
                    fe = fe.next;
                }
            }
        }
    }

    /**
     * Record re-enrollment.
     * <p>
     * <I>Note: this method should only be called by an AltingBarrier synchronised
     * on this AltingBarrierBase.</I>
     */
    void enroll() {
        enrolled++;
        countdown++;
    }

}
