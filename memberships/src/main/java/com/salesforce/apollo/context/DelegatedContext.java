package com.salesforce.apollo.context;

import com.salesforce.apollo.cryptography.Digest;
import com.salesforce.apollo.membership.Member;
import org.apache.commons.math3.random.BitsStreamGenerator;

import java.util.List;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Stream;

/**
 * @author hal.hildebrand
 **/
public class DelegatedContext<T extends Member> implements Context<T> {

    private Context<T> delegate;

    public DelegatedContext(Context<T> delegate) {
        this.delegate = delegate;
    }

    @Override
    public Stream<T> allMembers() {
        return delegate.allMembers();
    }

    @Override
    public Iterable<T> betweenPredecessors(int ring, T start, T stop) {
        return delegate.betweenPredecessors(ring, start, stop);
    }

    @Override
    public Iterable<T> betweenSuccessor(int ring, T start, T stop) {
        return delegate.betweenSuccessor(ring, start, stop);
    }

    @Override
    public int cardinality() {
        return delegate.cardinality();
    }

    public <Q extends Context<T>> Q delegate() {
        return (Q) delegate;
    }

    @Override
    public int diameter() {
        return delegate.diameter();
    }

    @Override
    public T findPredecessor(int ring, Digest d, Function<T, IterateResult> predicate) {
        return delegate.findPredecessor(ring, d, predicate);
    }

    @Override
    public T findPredecessor(int ring, T m, Function<T, IterateResult> predicate) {
        return delegate.findPredecessor(ring, m, predicate);
    }

    @Override
    public T findSuccessor(int ring, Digest d, Function<T, IterateResult> predicate) {
        return delegate.findSuccessor(ring, d, predicate);
    }

    @Override
    public T findSuccessor(int ring, T m, Function<T, IterateResult> predicate) {
        return delegate.findSuccessor(ring, m, predicate);
    }

    @Override
    public List<T> getAllMembers() {
        return delegate.getAllMembers();
    }

    @Override
    public int getBias() {
        return delegate.getBias();
    }

    @Override
    public double getEpsilon() {
        return delegate.getEpsilon();
    }

    @Override
    public Digest getId() {
        return delegate.getId();
    }

    @Override
    public T getMember(Digest memberID) {
        return delegate.getMember(memberID);
    }

    @Override
    public T getMember(int i, int ring) {
        return delegate.getMember(i, ring);
    }

    @Override
    public double getProbabilityByzantine() {
        return delegate.getProbabilityByzantine();
    }

    @Override
    public short getRingCount() {
        return delegate.getRingCount();
    }

    @Override
    public Digest hashFor(Digest d, int ring) {
        return delegate.hashFor(d, ring);
    }

    @Override
    public Digest hashFor(T m, int ring) {
        return delegate.hashFor(m, ring);
    }

    @Override
    public boolean isBetween(int ring, T predecessor, T item, T successor) {
        return delegate.isBetween(ring, predecessor, item, successor);
    }

    @Override
    public boolean isMember(Digest digest) {
        return delegate.isMember(digest);
    }

    @Override
    public boolean isMember(T m) {
        return delegate.isMember(m);
    }

    @Override
    public boolean isSuccessorOf(T m, Digest digest) {
        return delegate.isSuccessorOf(m, digest);
    }

    @Override
    public int majority() {
        return delegate.majority();
    }

    public int majority(boolean bootstrapped) {
        return delegate.majority();
    }

    @Override
    public int memberCount() {
        return delegate.memberCount();
    }

    @Override
    public T predecessor(int ring, Digest location) {
        return delegate.predecessor(ring, location);
    }

    @Override
    public T predecessor(int ring, Digest location, Predicate<T> predicate) {
        return delegate.predecessor(ring, location, predicate);
    }

    @Override
    public T predecessor(int ring, T m) {
        return delegate.predecessor(ring, m);
    }

    @Override
    public T predecessor(int ring, T m, Predicate<T> predicate) {
        return delegate.predecessor(ring, m, predicate);
    }

    @Override
    public List<T> predecessors(Digest key) {
        return delegate.predecessors(key);
    }

    @Override
    public List<T> predecessors(Digest key, Predicate<T> test) {
        return delegate.predecessors(key, test);
    }

    @Override
    public List<T> predecessors(T key) {
        return delegate.predecessors(key);
    }

    @Override
    public List<T> predecessors(T key, Predicate<T> test) {
        return delegate.predecessors(key, test);
    }

    @Override
    public Iterable<T> predecessors(int ring, Digest location) {
        return delegate.predecessors(ring, location);
    }

    @Override
    public Iterable<T> predecessors(int ring, Digest location, Predicate<T> predicate) {
        return delegate.predecessors(ring, location, predicate);
    }

    @Override
    public Iterable<T> predecessors(int ring, T start) {
        return delegate.predecessors(ring, start);
    }

    @Override
    public Iterable<T> predecessors(int ring, T start, Predicate<T> predicate) {
        return delegate.predecessors(ring, start, predicate);
    }

    @Override
    public int rank(int ring, Digest item, Digest dest) {
        return delegate.rank(ring, item, dest);
    }

    @Override
    public int rank(int ring, Digest item, T dest) {
        return delegate.rank(ring, item, dest);
    }

    @Override
    public int rank(int ring, T item, T dest) {
        return delegate.rank(ring, item, dest);
    }

    @Override
    public <N extends T> List<T> sample(int range, BitsStreamGenerator entropy, Digest exc) {
        return delegate.sample(range, entropy, exc);
    }

    @Override
    public <N extends T> List<T> sample(int range, BitsStreamGenerator entropy, Predicate<T> excluded) {
        return delegate.sample(range, entropy, excluded);
    }

    public void setContext(Context<T> delegate) {
        this.delegate = delegate;
    }

    @Override
    public int size() {
        return delegate.size();
    }

    @Override
    public Stream<T> stream(int ring) {
        return delegate.stream(ring);
    }

    @Override
    public Stream<T> streamPredecessors(int ring, Digest location, Predicate<T> predicate) {
        return delegate.streamPredecessors(ring, location, predicate);
    }

    @Override
    public Stream<T> streamPredecessors(int ring, T m, Predicate<T> predicate) {
        return delegate.streamPredecessors(ring, m, predicate);
    }

    @Override
    public Stream<T> streamSuccessors(int ring, Digest location, Predicate<T> predicate) {
        return delegate.streamSuccessors(ring, location, predicate);
    }

    @Override
    public Stream<T> streamSuccessors(int ring, T m, Predicate<T> predicate) {
        return delegate.streamSuccessors(ring, m, predicate);
    }

    @Override
    public T successor(int ring, Digest hash) {
        return delegate.successor(ring, hash);
    }

    @Override
    public T successor(int ring, Digest hash, Predicate<T> predicate) {
        return delegate.successor(ring, hash, predicate);
    }

    @Override
    public T successor(int ring, T m) {
        return delegate.successor(ring, m);
    }

    @Override
    public T successor(int ring, T m, Predicate<T> predicate) {
        return delegate.successor(ring, m, predicate);
    }

    @Override
    public List<T> successors(Digest key) {
        return delegate.successors(key);
    }

    @Override
    public List<T> successors(Digest key, Predicate<T> test) {
        return delegate.successors(key, test);
    }

    @Override
    public List<T> successors(T key) {
        return delegate.successors(key);
    }

    @Override
    public List<T> successors(T key, Predicate<T> test) {
        return delegate.successors(key, test);
    }

    @Override
    public Iterable<T> successors(int ring, Digest location) {
        return delegate.successors(ring, location);
    }

    @Override
    public Iterable<T> successors(int ring, Digest location, Predicate<T> predicate) {
        return delegate.successors(ring, location, predicate);
    }

    @Override
    public Iterable<T> successors(int ring, T m, Predicate<T> predicate) {
        return delegate.successors(ring, m, predicate);
    }

    @Override
    public int timeToLive() {
        return delegate.timeToLive();
    }

    @Override
    public int toleranceLevel() {
        return delegate.toleranceLevel();
    }

    @Override
    public Iterable<T> traverse(int ring, T member) {
        return delegate.traverse(ring, member);
    }

    @Override
    public void uniqueSuccessors(Digest key, Predicate<T> test, Set<T> collector) {
        delegate.uniqueSuccessors(key, test, collector);
    }

    @Override
    public void uniqueSuccessors(Digest key, Set<T> collector) {
        delegate.uniqueSuccessors(key, collector);
    }

    @Override
    public boolean validRing(int ring) {
        return delegate.validRing(ring);
    }
}
