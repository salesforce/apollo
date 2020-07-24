/*
 * Copyright 2012-2014 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.calcite.adapter.jdbc.programs;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.util.Arrays;
import java.util.List;

import org.apache.calcite.plan.RelOptLattice;
import org.apache.calcite.plan.RelOptMaterialization;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.tools.Program;
import org.apache.calcite.util.Holder;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import com.google.common.collect.ImmutableList;

public class SequenceProgramTest {
    private RelOptPlanner planner;

    private RelTraitSet relTraitSet;

    private Program program;

    private Program subProgram;

    private RelNode inNode;

    private List<RelOptMaterialization> relOptMaterializationList;

    private List<RelOptLattice> relOptLatticeList;

    @BeforeEach
    public void setupMocks() {
        planner = Mockito.mock(RelOptPlanner.class);
        relTraitSet = RelTraitSet.createEmpty();
        subProgram = Mockito.mock(Program.class);
        program = new SequenceProgram(subProgram);
        inNode = Mockito.mock(RelNode.class);
        relOptMaterializationList = Arrays.asList();
        relOptLatticeList = Arrays.asList();
    }

    @Test
    public void testEmptyProgram_doesNothing() {
        program = new SequenceProgram();

        RelNode result = program.run(planner, inNode, relTraitSet, relOptMaterializationList, relOptLatticeList);

        assertSame(inNode, result);
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testRun_propagatesToSubProgram() {
        RelNode node2 = Mockito.mock(RelNode.class);
        Mockito.doReturn(node2)
               .when(subProgram)
               .run(Mockito.any(), Mockito.same(inNode), Mockito.any(), Mockito.anyList(), Mockito.anyList());

        RelNode result = program.run(planner, inNode, relTraitSet, relOptMaterializationList, relOptLatticeList);

        assertSame(node2, result);
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testRun_propagatesAllParameters() {
        RelNode node2 = Mockito.mock(RelNode.class);
        Mockito.doReturn(node2)
               .when(subProgram)
               .run(Mockito.any(), Mockito.same(inNode), Mockito.any(), Mockito.anyList(), Mockito.anyList());

        program.run(planner, inNode, relTraitSet, relOptMaterializationList, relOptLatticeList);

        Mockito.verify(subProgram)
               .run(Mockito.same(planner), Mockito.any(), Mockito.same(relTraitSet), Mockito.anyList(),
                    Mockito.anyList());
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testRun_chainsSubPrograms() {
        Program subProgram2 = Mockito.mock(Program.class);
        RelNode node2 = Mockito.mock(RelNode.class);
        RelNode node3 = Mockito.mock(RelNode.class);
        program = new SequenceProgram(subProgram, subProgram2);
        Mockito.doReturn(node2)
               .when(subProgram)
               .run(Mockito.any(), Mockito.same(inNode), Mockito.any(), Mockito.anyList(), Mockito.anyList());
        Mockito.doReturn(node3)
               .when(subProgram2)
               .run(Mockito.any(), Mockito.same(node2), Mockito.any(), Mockito.anyList(), Mockito.anyList());

        RelNode result = program.run(planner, inNode, relTraitSet, relOptMaterializationList, relOptLatticeList);

        assertSame(node3, result);
    }

    @Test
    public void testPrepend_returnsAHookForAddingTheProgram() {
        Program originalProgram = Mockito.mock(Program.class);
        Holder<Program> holder = Holder.of(originalProgram);

        SequenceProgram.prepend(subProgram).accept(holder);

        Program newProgram = holder.get();
        assertTrue(newProgram instanceof SequenceProgram);
        ImmutableList<Program> subPrograms = ((SequenceProgram) newProgram).getPrograms();
        assertEquals(2, subPrograms.size());
        assertSame(subProgram, subPrograms.get(0));
        assertSame(originalProgram, subPrograms.get(1));
    }

    @Test
    public void testPrepend_usesTheDefaultProgramIfGivenNull() {
        Holder<Program> holder = Holder.of(null);

        SequenceProgram.prepend(subProgram).accept(holder);

        Program newProgram = holder.get();
        assertTrue(newProgram instanceof SequenceProgram);
        ImmutableList<Program> subPrograms = ((SequenceProgram) newProgram).getPrograms();
        // Not much we can check about the program; it is built inline and has no
        // particular features
        assertNotNull(subPrograms.get(1));
    }

    public void testPrepend_throwsIfNoHolderIsGiven() {
        try {
            SequenceProgram.prepend(subProgram).accept(null);
            fail("expected runtime exception");
        } catch (RuntimeException e) {
            // expected
        }
    }
}
