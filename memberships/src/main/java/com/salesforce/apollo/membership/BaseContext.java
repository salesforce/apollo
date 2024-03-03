package com.salesforce.apollo.membership;

import com.salesforce.apollo.cryptography.Digest;
import org.apache.commons.math3.random.BitsStreamGenerator;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Stream;

public interface BaseContext<T extends Member> {

    static List<Member> uniqueSuccessors(Context<Member> context, Digest digest) {
        Set<Member> post = new HashSet<>();
        context.successors(digest, m -> {
            if (post.size() == context.getRingCount()) {
                return false;
            }
            return post.add(m);
        });
        var successors = new ArrayList<>(post);
        return successors;
    }

    /**
     * Answer a stream over all members, offline and active
     */
    Stream<T> allMembers();

    /**
     * Maximum cardinality of this context
     */
    int cardinality();

    /**
     * Answer the aproximate diameter of the receiver, assuming the rings were built with FF parameters, with the rings
     * forming random graph connections segments.
     */
    int diameter();

    /**
     * @return the List of all members
     */
    List<T> getAllMembers();

    /**
     * Answer the bias of the context. The bias is the multiple of the number of byzantine members the context is
     * designed to foil
     */
    int getBias();

    double getEpsilon();

    /**
     * Answer the identifier of the context
     */
    Digest getId();

    /**
     * Answer the member matching the id, or null if none.
     */
    T getMember(Digest memberID);

    /**
     * Answer the probability {0, 1} that any given member is byzantine
     */
    double getProbabilityByzantine();

    /**
     * Answer the number of rings in the context
     */
    short getRingCount();

    default Digest hashFor(Digest d, int ring) {
        return Context.hashFor(getId(), ring, d);
    }

    Digest hashFor(T m, int ring);

    /**
     * Answer true if a member who's id is the supplied digest is a member of the view
     */
    boolean isMember(Digest digest);

    /**
     * Answer true if is a member of the view
     */
    boolean isMember(T m);

    /**
     * Answer true if the member is a successor of the supplied digest on any ring
     *
     * @param m
     * @param digest
     * @return
     */
    boolean isSuccessorOf(T m, Digest digest);

    /**
     * Answer the majority cardinality of the context, based on the current ring count
     */
    default int majority() {
        return getRingCount() - toleranceLevel();
    }

    /**
     * Answer the majority cardinality of the context, based on the current ring count
     *
     * @param bootstrapped - if true, calculate correct majority for bootstrapping cases where totalCount < true
     *                     majority
     */
    int majority(boolean bootstrapped);

    /**
     * Answer the total member count (offline + active) tracked by this context
     */
    int memberCount();

    /**
     * @return the predecessor on each ring for the provided key
     */
    List<T> predecessors(Digest key);

    /**
     * @return the predecessor on each ring for the provided key that pass the provided predicate
     */
    List<T> predecessors(Digest key, Predicate<T> test);

    /**
     * @return the predecessor on each ring for the provided key
     */
    List<T> predecessors(T key);

    /**
     * @return the predecessor on each ring for the provided key that pass the provided predicate
     */
    List<T> predecessors(T key, Predicate<T> test);

    /**
     * Answer a random sample of at least range size from the active members of the context
     *
     * @param range   - the desired range
     * @param entropy - source o randomness
     * @param exc     - the member to exclude from sample
     * @return a random sample set of the view's live members. May be limited by the number of active members.
     */
    <N extends T> List<T> sample(int range, BitsStreamGenerator entropy, Digest exc);

    /**
     * Answer a random sample of at least range size from the active members of the context
     *
     * @param range    - the desired range
     * @param entropy  - source o randomness
     * @param excluded - the member to exclude from sample
     * @return a random sample set of the view's live members. May be limited by the number of active members.
     */
    <N extends T> List<T> sample(int range, BitsStreamGenerator entropy, Predicate<T> excluded);

    /**
     * Answer the total count of active and offline members of this context
     */
    int size();

    /**
     * @return the list of successors to the key on each ring
     */
    List<T> successors(Digest key);

    /**
     * @return the list of successor to the key on each ring that pass the provided predicate test
     */
    List<T> successors(Digest key, Predicate<T> test);

    /**
     * @return the list of successors to the key on each ring
     */
    List<T> successors(T key);

    /**
     * @return the list of successor to the key on each ring that pass the provided predicate test
     */
    List<T> successors(T key, Predicate<T> test);

    /**
     * The number of iterations until a given message has been distributed to all members in the context, using the
     * rings of the receiver as a gossip graph
     */
    int timeToLive();

    /**
     * Answer the tolerance level of the context to byzantine members, assuming this context has been constructed from
     * FF parameters
     */
    int toleranceLevel();

    /**
     * @return the total number of members
     */
    int totalCount();

    boolean validRing(int ring);
}
