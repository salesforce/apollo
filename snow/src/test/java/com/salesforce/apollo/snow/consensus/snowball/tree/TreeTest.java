package com.salesforce.apollo.snow.consensus.snowball.tree;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;
import static com.salesforce.apollo.snow.consensus.snowball.ConsensusTest.*;
import com.salesforce.apollo.snow.consensus.snowball.Parameters;
import com.salesforce.apollo.snow.ids.Bag;
import com.salesforce.apollo.snow.ids.ID;

public class TreeTest {

    @Test
    public void singleton() {
        Parameters params = Parameters.newBuilder().setK(1).setAlpha(1).setBetaVirtuous(2).setBetaRogue(5).build();
        Tree tree = new Tree(params, Red);

        assertFalse(tree.finalized());

        Bag oneRed = new Bag();
        oneRed.add(Red);
        tree.recordPoll(oneRed);

        assertFalse(tree.finalized());

        Bag empty = new Bag();
        tree.recordPoll(empty);

        assertFalse(tree.finalized());

        tree.recordPoll(oneRed);

        assertFalse(tree.finalized());

        tree.recordPoll(oneRed);

        assertTrue(tree.finalized());
        assertEquals(Red, tree.preference());

        tree.add(Blue);

        Bag oneBlue = new Bag();
        oneBlue.add(Blue);
        tree.recordPoll(oneBlue);

        assertTrue(tree.finalized());
        assertEquals(Red, tree.preference());
    }

    @Test
    public void snowball5Colors() {
        int numColors = 5;
        Parameters params = new Parameters("", null, 5, 5, 20, 30, 1);

        ID[] colors = new ID[numColors];
        for (int i = 0; i < numColors; i++) {
            colors[i] = new ID(i);
        }

        Tree tree0 = new Tree(params, colors[4]);

        tree0.add(colors[0]);
        tree0.add(colors[1]);
        tree0.add(colors[2]);
        tree0.add(colors[3]);

        Tree tree1 = new Tree(params, colors[3]);

        tree1.add(colors[0]);
        tree1.add(colors[1]);
        tree1.add(colors[2]);
        tree1.add(colors[4]);
        // todo compare
    }

    @Test
    public void addPreviouslyRejected() {
        ID zero = new ID((byte) 0);
        ID one = new ID((byte) 1);
        ID two = new ID((byte) 2);
        ID four = new ID((byte) 4);

        Parameters params = new Parameters("", null, 1, 1, 1, 2, 1);
        Tree tree = new Tree(params, zero);
        tree.add(one);
        tree.add(four);

        ID pref = tree.preference();
        assertEquals(zero, pref);
        assertFalse(tree.finalized());

        Bag zeroBag = new Bag();
        zeroBag.add(zero);
        tree.recordPoll(zeroBag);

        pref = tree.preference();
        assertEquals(zero, pref);
        assertFalse(tree.finalized());

        tree.add(two);

        pref = tree.preference();
        assertEquals(zero, pref);
        assertFalse(tree.finalized());
    }

    @Test
    public void binary() {
        Parameters params = new Parameters("", null, 1, 1, 1, 2, 1);
        Tree tree = new Tree(params, Red);
        tree.add(Blue);

        assertEquals(Red, tree.preference());
        assertFalse(tree.finalized());

        Bag oneBlue = new Bag();
        oneBlue.add(Blue);
        tree.recordPoll(oneBlue);

        assertEquals(Blue, tree.preference());
        assertFalse(tree.finalized());

        Bag oneRed = new Bag();
        oneRed.add(Red);
        tree.recordPoll(oneRed);

        assertEquals(Blue, tree.preference());
        assertFalse(tree.finalized());

        tree.recordPoll(oneBlue);

        assertEquals(Blue, tree.preference());
        assertFalse(tree.finalized());

        tree.recordPoll(oneBlue);

        assertEquals(Blue, tree.preference());
        assertTrue(tree.finalized());
    }
}
