/*
 * Copyright (c) 2020, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.snow.consensus.snowman;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import com.salesforce.apollo.snow.Context;
import com.salesforce.apollo.snow.consensus.snowball.Parameters;
import com.salesforce.apollo.snow.ids.Bag;
import com.salesforce.apollo.snow.ids.ID;

/**
 * @author hal.hildebrand
 * @param <Vote>
 *
 */
public class Topological implements Consensus {
    public static class TopologicalFactory implements Consensus.Factory {

        @Override
        public Consensus construct(Context context, Parameters params, ID rootId, Metrics metrics) {
            return new Topological(context, params, rootId, metrics);
        }

    }

    private static class DegreesResult {
        private Deque<ID>         leaves;
        private Map<ID, kahnNode> reachable;

        public DegreesResult(Map<ID, kahnNode> reachable, Deque<ID> leaves) {
            this.reachable = reachable;
            this.leaves = leaves;
        }
    }

    // Used to track the kahn topological sort status
    private static class kahnNode {
        // inDegree is the number of children that haven't been processed yet. If
        // inDegree is 0, then this node is a leaf
        int inDegree;
        // votes for all the children of this node, so far
        Bag votes;
    }

    private static class votes {
        ID  parentID;
        Bag votes;

        public votes(ID parentID, Bag votes) {
            this.parentID = parentID;
            this.votes = votes;
        }
    }

    // blocks stores the last accepted block and all the pending blocks
    private final Map<ID, SnowmanBlock> blocks = new HashMap<>();
    private Context                     ctx;
    // head is the last accepted block
    private ID      head;
    private Metrics metrics;

    private Parameters params;

    // tail is the preferred block with no children
    private ID tail;

    public Topological(Context context, Parameters params, ID rootId, Metrics metrics) {
        this.metrics = metrics;
        this.ctx = context;
        this.params = params;
        this.head = rootId;
        this.tail = rootId;
        blocks.put(rootId, new SnowmanBlock(this));
    }

    @Override
    public void add(Block blk) {
        Block parent = blk.parent();
        ID parentID = parent.id();

        ID blkID = blk.id();
        byte[] blkBytes = blk.bytes();

        // Notify anyone listening that this block was issued.
        ctx.decisionDispatcher.issue(ctx, blkID, blkBytes);
        ctx.consensusDispatcher.issue(ctx, blkID, blkBytes);
        metrics.issued(blkID);

        SnowmanBlock parentNode = blocks.get(parentID);
        if (parentNode == null) {
            // If the ancestor is missing, this means the ancestor must have already
            // been pruned. Therefore, the dependent should be transitively
            // rejected.
            blk.reject();
            // Notify anyone listening that this block was rejected.
            ctx.decisionDispatcher.reject(ctx, blkID, blkBytes);
            ctx.consensusDispatcher.reject(ctx, blkID, blkBytes);
            metrics.rejected(blkID);
            return;
        }

        // add the block as a child of its parent, and add the block to the tree
        parentNode.addChild(blk);
        blocks.put(blkID, new SnowmanBlock(this, blk));

        // If we are extending the tail, this is the new tail
        if (tail.equals(parentID)) {
            tail = blkID;
        }
    }

    @Override
    public boolean finalized() {
        return blocks.size() == 1;
    }

    @Override
    public boolean issued(Block blk) {
        // If the block is decided, then it must have been previously issued.
        if (blk.status().decided()) {
            return true;
        }
        // If the block is in the map of current blocks, then the block was issued.
        return blocks.containsKey(blk.id());
    }

    @Override
    public Parameters parameters() {
        return params;
    }

    @Override
    public ID preference() {
        return tail;
    }

    /**
     * The votes bag contains at most K votes for blocks in the tree. If there is a
     * vote for a block that isn't in the tree, the vote is dropped.
     * 
     * Votes are propagated transitively towards the genesis. All blocks in the tree
     * that result in at least Alpha votes will record the poll on their children.
     * Every other block will have an unsuccessful poll registered.
     * 
     * After collecting which blocks should be voted on, the polls are registered
     * and blocks are accepted/rejected as needed. The tail is then updated to equal
     * the leaf on the preferred branch.
     * 
     * To optimize the theoretical complexity of the vote propagation, a topological
     * sort is done over the blocks that are reachable from the provided votes.
     * During the sort, votes are pushed towards the genesis. To prevent interating
     * over all blocks that had unsuccessful polls, we set a flag on the block to
     * know that any future traversal through that block should register an
     * unsuccessful poll on that block and every descendant block.
     * 
     * The complexity of this function is: - Runtime = 3 * |live set| + |votes| -
     * Space = 2 * |live set| + |votes|
     * 
     */
    @Override
    public void recordPoll(Bag voteBag) {
        Deque<votes> voteStack = new ArrayDeque<>();
        if (voteBag.size() >= params.alpha) {
            // If there is no way for an alpha majority to occur, there is no need
            // to perform any traversals.

            // Runtime = |live set| + |votes| ; Space = |live set| + |votes|
            DegreesResult result = calculateInDegree(voteBag);

            // Runtime = |live set| ; Space = |live set|
            voteStack = pushVotes(result.reachable, result.leaves);
        }

        // Runtime = |live set| ; Space = Constant
        ID preferred = vote(voteStack);
        if (preferred == null) {
            throw new IllegalStateException();
        }

        // Runtime = |live set| ; Space = Constant
        tail = getPreferredDecendent(preferred);
    }

    // accept the preferred child of the provided snowman block. By accepting the
    // preferred child, all other children will be rejected. When these children are
    // rejected, all their descendants will be rejected.
    private boolean accept(SnowmanBlock n) {
        // We are finalizing the block's child, so we need to get the preference
        ID pref = n.getSb().preference();

        ctx.log.info("Accepting block with ID %s", pref);

        // Get the child and accept it
        Block child = n.getChild(pref);
        child.accept();

        // Notify anyone listening that this block was accepted.
        byte[] bytes = child.bytes();
        ctx.decisionDispatcher.accept(ctx, pref, bytes);
        ctx.consensusDispatcher.accept(ctx, pref, bytes);
        metrics.accepted(pref);

        // Because this is the newest accepted block, this is the new head.
        head = pref;

        // Because ts.blocks contains the last accepted block, we don't delete the
        // block from the blocks map here.

        Deque<ID> rejects = new ArrayDeque<ID>();
        for (Entry<ID, Block> e : n.getChildren().entrySet()) {
            if (!e.getKey().equals(pref)) {
                // don't reject the block we just accepted

                e.getValue().reject();

                // Notify anyone listening that this block was rejected.
                byte[] b = e.getValue().bytes();
                ctx.decisionDispatcher.reject(ctx, e.getKey(), b);
                ctx.consensusDispatcher.reject(ctx, e.getKey(), b);
                metrics.rejected(e.getKey());

                // Track which blocks have been directly rejected
                rejects.add(e.getKey());
            }

        }

        // reject all the descendants of the blocks we just rejected
        rejectTransitively(rejects);
        return true;
    }

    // takes in a list of votes and sets up the topological ordering. Returns the
    // reachable section of the graph annotated with the number of inbound edges and
    // the non-transitively applied votes. Also returns the list of leaf blocks.
    private DegreesResult calculateInDegree(Bag votes) {
        Map<ID, kahnNode> kahns = new HashMap<>();
        Deque<ID> leaves = new ArrayDeque<>();

        for (ID vote : votes.list()) {
            SnowmanBlock votedBlock = blocks.get(vote);

            // If the vote is for a block that isn't in the current pending set,
            // then the vote is dropped
            if (votedBlock == null) {
                continue;
            }

            // If the vote is for the last accepted block, the vote is dropped
            if (votedBlock.accepted()) {
                continue;
            }

            // The parent contains the snowball instance of its children
            Block parent = votedBlock.getBlock().parent();
            ID parentID = parent.id();

            // Add the votes for this block to the parent's set of responces
            int numVotes = votes.count(vote);
            kahnNode kahn = kahns.get(parentID);

            if (kahn != null) {
                // If the parent block already had registered votes, then there is no
                // need to iterate into the parents
                kahn.votes.addCount(vote, numVotes);
                kahns.put(parentID, kahn);
                continue;
            }

            // If I've never seen this parent block before, it is currently a leaf.
            leaves.add(parentID);

            // iterate through all the block's ancestors and set up the inDegrees of
            // the blocks
            for (SnowmanBlock n = blocks.get(parentID); !n.accepted(); n = blocks.get(parentID)) {
                parent = n.getBlock().parent();
                parentID = parent.id();

                // Increase the inDegree by one
                kahn = kahns.get(parentID);
                kahn.inDegree++;
                kahns.put(parentID, kahn);

                // If we have already seen this block, then we shouldn't increase
                // the inDegree of the ancestors through this block again.
                if (kahn.inDegree != 1) {
                    break;
                }

                // If I am transitively seeing this block for the first time, either
                // the block was previously unknown or it was previously a leaf.
                // Regardless, it shouldn't be tracked as a leaf.
                leaves.remove(parentID);
            }
        }

        return new DegreesResult(kahns, leaves);
    }

    // Get the preferred decendent of the provided block ID
    private ID getPreferredDecendent(ID blkID) {
        // Traverse from the provided ID to the preferred child until there are no
        // children.
        for (SnowmanBlock block = blocks.get(blkID); block.getSb() != null; block = blocks.get(blkID)) {
            blkID = block.getSb().preference();
        }
        return blkID;
    }

    // convert the tree into a branch of snowball instances with at least alpha
    // votes
    private Deque<votes> pushVotes(Map<ID, kahnNode> kahnNodes, Deque<ID> leaves) {
        Deque<votes> voteStack = new ArrayDeque<>();
        while (!leaves.isEmpty()) {
            // pop a leaf off the stack
            ID leafID = leaves.pop();

            // get the block and sort infomation about the block
            kahnNode kn = kahnNodes.get(leafID);
            SnowmanBlock block = blocks.get(leafID);

            // If there are at least Alpha votes, then this block needs to record
            // the poll on the snowball instance
            if (kn.votes.size() >= params.alpha) {
                voteStack.add(new votes(leafID, kn.votes));
            }

            // If the block is accepted, then we don't need to push votes to the
            // parent block
            if (block.accepted()) {
                continue;
            }

            Block parent = block.getBlock().parent();
            ID parentID = parent.id();

            // Remove an inbound edge from the parent kahn node and push the votes.
            kahnNode parentKahnNode = kahnNodes.get(parentID);
            parentKahnNode.inDegree--;
            parentKahnNode.votes.addCount(leafID, kn.votes.size());
            kahnNodes.put(parentID, parentKahnNode);

            // If the inDegree is zero, then the parent node is now a leaf
            if (parentKahnNode.inDegree == 0) {
                leaves.add(parentID);
            }
        }
        return voteStack;
    }

    private void rejectTransitively(Deque<ID> rejected) {
        // the rejected array is treated as a queue, with the next element at index
        // 0 and the last element at the end of the slice.
        while (rejected.size() > 0) {
            // pop the rejected ID off the queue
            ID rejectedID = rejected.pop();

            // get the rejected node, and remove it from the tree
            SnowmanBlock rejectedNode = blocks.remove(rejectedID);
            rejectedNode.forEachChild((id, child) -> {
                child.reject();

                // Notify anyone listening that this block was rejected.
                byte[] bytes = child.bytes();
                ctx.decisionDispatcher.reject(ctx, id, bytes);
                ctx.consensusDispatcher.reject(ctx, id, bytes);
                metrics.rejected(id);

                // add the newly rejected block to the end of the queue
                rejected.add(id);
            });
        }
    }

    // apply votes to the branch that received an Alpha threshold
    private ID vote(Deque<votes> voteStack) {
        // If the voteStack is empty, then the full tree should falter. This won't
        // change the preferred branch.
        if (voteStack.isEmpty()) {
            ctx.log.info("No progress was made after a vote with {} pending blocks", blocks.size() - 1);

            SnowmanBlock headBlock = blocks.get(head);
            headBlock.setShouldFalter(true);
            return tail;
        }

        // keep track of the new preferred block
        ID newPreferred = head;
        boolean onPreferredBranch = true;
        while (!voteStack.isEmpty()) {
            // pop a vote off the stack
            votes vote = voteStack.pop();

            // get the block that we are going to vote on
            SnowmanBlock parentBlock = blocks.get(vote.parentID);

            // if the block block we are going to vote on was already rejected, then
            // we should stop applying the votes
            if (parentBlock == null) {
                break;
            }

            // keep track of transitive falters to propagate to this block's
            // children
            boolean shouldTransitivelyFalter = parentBlock.isShouldFalter();

            // if the block was previously marked as needing to falter, the block
            // should falter before applying the vote
            if (shouldTransitivelyFalter) {
                ctx.log.info("Resetting confidence below {}", vote.parentID);

                parentBlock.getSb().recordUnsuccessfulPoll();
                parentBlock.setShouldFalter(false);
            }

            // apply the votes for this snowball instance
            parentBlock.getSb().recordPoll(vote.votes);

            // Only accept when you are finalized and the head.
            if (parentBlock.getSb().finalized() && head.equals(vote.parentID)) {
                if (!accept(parentBlock)) {
                    return null;
                }

                // by accepting the child of parentBlock, the last accepted block is
                // no longer voteParentID, but its child. So, voteParentID can be
                // removed from the tree.
                blocks.remove(vote.parentID);
            }

            // If we are on the preferred branch, then the parent's preference is
            // the next block on the preferred branch.
            ID parentPreference = parentBlock.getSb().preference();
            if (onPreferredBranch) {
                newPreferred = parentPreference;
            }

            // Get the ID of the child that is having a RecordPoll called. All other
            // children will need to have their confidence reset. If there isn't a
            // child having RecordPoll called, then the nextID will default to the
            // nil ID.
            ID nextID = ID.ORIGIN;
            if (!voteStack.isEmpty()) {
                nextID = voteStack.peek().parentID;
            }

            // If we are on the preferred branch and the nextID is the preference of
            // the snowball instance, then we are following the preferred branch.
            onPreferredBranch = onPreferredBranch && nextID.equals(parentPreference);

            // If there wasn't an alpha threshold on the branch (either on this vote
            // or a past transitive vote), I should falter now.
            for (ID childID : parentBlock.getChildren().keySet()) {
                // If we don't need to transitively falter and the child is going to
                // have RecordPoll called on it, then there is no reason to reset
                // the block's confidence
                if (!shouldTransitivelyFalter && childID.equals(nextID)) {
                    continue;
                }

                // If we finalized a child of the current block, then all other
                // children will have been rejected and removed from the tree.
                // Therefore, we need to make sure the child is still in the tree.
                SnowmanBlock childBlock = blocks.get(childID);
                if (childBlock != null) {
                    ctx.log.info("Defering confidence reset of {}. Voting for {}", childID, nextID);

                    // If the child is ever voted for positively, the confidence
                    // must be reset first.
                    childBlock.setShouldFalter(true);
                }
            }
        }
        return newPreferred;
    }
}
