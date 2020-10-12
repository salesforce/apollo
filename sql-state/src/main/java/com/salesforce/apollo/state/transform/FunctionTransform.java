/*
 * Copyright (c) 2020, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.apollo.state.transform;

import java.util.List;

import org.apache.calcite.plan.RelOptLattice;
import org.apache.calcite.plan.RelOptMaterialization;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.tools.Program;

/**
 * @author hal.hildebrand
 *
 */
public class FunctionTransform implements Program {

    @Override
    public RelNode run(RelOptPlanner planner, RelNode rel, RelTraitSet requiredOutputTraits,
                       List<RelOptMaterialization> materializations, List<RelOptLattice> lattices) {
        // TODO Auto-generated method stub
        return null;
    }

}
