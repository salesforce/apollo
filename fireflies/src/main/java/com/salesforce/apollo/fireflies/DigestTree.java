/*
 * Copyright (c) 2022, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.fireflies;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import com.salesforce.apollo.crypto.Digest;

/**
 * A compact Merkle tree for lists of Digests. The internal hashes are integers
 * and are simply the XOR of the hashes of children.
 *
 * @author hal.hildebrand
 *
 */
public class DigestTree implements Iterable<DigestTree.NodeEntry> {

    public record NodeEntry(int hash, Digest leaf) {
        public boolean isLeaf() {
            return leaf != null;
        }

    }

    interface Node {
        int hash();

        Node left();

        Node right();
    }

    record Internal(int hash, Node left, Node right) implements Node {}

    private static class LeafNode implements Node {
        private final Digest value;

        public LeafNode(Digest value) {
            this.value = value;
        }

        @Override
        public int hash() {
            return DigestTree.hash(value);
        }

        @Override
        public Node left() {
            return null;
        }

        @Override
        public Node right() {
            return null;
        }

    }

    private record LeafParent(int hash, int leftIndex, int rightIndex) implements Node {

        @Override
        public Node left() {
            return null;
        }

        @Override
        public Node right() {
            return null;
        }
    }

    private static int hash(Digest d) {
        long current = 0;
        for (var l : d.getLongs()) {
            current ^= l;
        }
        return Long.hashCode(current);
    }

    private final List<Digest> digests;
    private final short        height;

    private final int nnodes;

    private final Node root;

    public DigestTree(List<Digest> digests) {
        if (digests.size() <= 1) {
            throw new IllegalArgumentException("Must be at least two signatures to construct a Merkle tree");
        }
        this.digests = digests;

        var count = digests.size();
        var parents = bottomLevel();
        count += parents.size();
        short depth = 1;

        while (parents.size() > 1) {
            parents = internalLevel(parents);
            depth++;
            count += parents.size();
        }
        height = depth;
        nnodes = count;
        root = parents.get(0);
    }

    public List<Digest> getDigests() {
        return digests;
    }

    public short getHeight() {
        return height;
    }

    public int getNnodes() {
        return nnodes;
    }

    public Node getRoot() {
        return root;
    }

    /**
     * 
     * @return the breadth first iterator of all nodes in the tree
     */
    @Override
    public Iterator<NodeEntry> iterator() {
        var deque = new ArrayDeque<Node>();
        deque.add(root);
        return new Iterator<NodeEntry>() {

            @Override
            public boolean hasNext() {
                return !deque.isEmpty();
            }

            @Override
            public NodeEntry next() {
                var n = deque.removeFirst();
                if (n instanceof LeafParent lp) {
                    deque.add(new LeafNode(digests.get(lp.leftIndex)));
                    if (lp.leftIndex != lp.rightIndex) {
                        deque.add(new LeafNode(digests.get(lp.rightIndex)));
                    }
                } else if (n instanceof LeafNode ln) {
                    return new NodeEntry(ln.hash(), ln.value);
                } else {
                    if (n.left() != null) {
                        deque.add(n.left());
                    }
                    if (n.right() != null) {
                        deque.add(n.right());
                    }
                }
                return new NodeEntry(n.hash(), null);
            }
        };
    }

    public Stream<NodeEntry> stream() {
        return StreamSupport.stream(Spliterators.spliteratorUnknownSize(iterator(), Spliterator.ORDERED), false);
    }

    private List<Node> bottomLevel() {
        List<Node> parents = new ArrayList<Node>(digests.size() / 2);

        for (int i = 0; i < digests.size() - 1; i += 2) {
            parents.add(new LeafParent(hash(digests.get(i)) ^ hash(digests.get(i + 1)), i, i + 1));
        }

        if (digests.size() % 2 != 0) {
            parents.add(new LeafParent(hash(digests.get(digests.size() - 1)), digests.size() - 1, digests.size() - 1));
        }

        return parents;
    }

    private List<Node> internalLevel(List<Node> children) {
        List<Node> parents = new ArrayList<Node>(children.size() / 2);

        for (int i = 0; i < children.size() - 1; i += 2) {
            Node left = children.get(i);
            Node right = children.get(i + 1);

            Node parent = new Internal(left.hash() ^ right.hash(), left, right);
            parents.add(parent);
        }

        if (children.size() % 2 != 0) {
            Node child = children.get(children.size() - 1);
            Node parent = new Internal(child.hash(), child, null);
            parents.add(parent);
        }

        return parents;
    }
}
