
package io.quantumdb.core.backends.planner;

import static com.google.common.base.Preconditions.checkArgument;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import io.quantumdb.core.backends.planner.Operation.Type;
import io.quantumdb.core.schema.definitions.Table;
import io.quantumdb.core.schema.definitions.View;
import io.quantumdb.core.versioning.RefLog;

public class Plan {

    public static class Builder {
        private final MigrationState state;
        private final List<Step>     steps;

        private Builder(MigrationState state) {
            this.state = state;
            this.steps = Lists.newArrayList();
        }

        public Step addNullRecord(Set<Table> tables) {
            for (Step step : steps) {
                Operation operation = step.getOperation();
                if (operation.getTables().equals(tables) && operation.getType() == Type.ADD_NULL) {
                    return step;
                }
            }
            Step step = new Step(new Operation(tables, Type.ADD_NULL));
            steps.add(0, step);
            return step;
        }

        public Plan build(RefLog refLog, Set<Table> ghostTables, Set<View> views) {
            return new Plan(Lists.newArrayList(steps), refLog, ghostTables, views);
        }

        public Step copy(Table table, Set<String> columns) {
            checkArgument(!columns.isEmpty(), "You cannot copy 0 columns!");
            Set<String> toDo = state.getYetToBeMigratedColumns(table.getName());
            LinkedHashSet<String> filtered = Sets.newLinkedHashSet(Sets.intersection(columns, toDo));
            if (filtered.isEmpty()) {
                for (int i = steps.size() - 1; i >= 0; i--) {
                    Step step = steps.get(i);
                    Operation operation = step.getOperation();
                    if (operation.getTables().equals(Sets.newHashSet(table)) && operation.getType() == Type.COPY) {
                        return step;
                    }
                }
            }
            Step step = new Step(new Operation(table, filtered, Type.COPY));
            state.markColumnsAsMigrated(table.getName(), filtered);
            steps.add(step);
            return step;
        }

        public Step dropNullRecord(Set<Table> tables) {
            for (Step step : steps) {
                Operation operation = step.getOperation();
                if (operation.getTables().equals(tables) && operation.getType() == Type.DROP_NULL) {
                    return step;
                }
            }
            Step step = new Step(new Operation(tables, Type.DROP_NULL));
            steps.add(step);
            return step;
        }

        public Optional<Step> findFirstCopy(Table table) {
            return steps.stream().filter(step -> {
                Operation operation = step.getOperation();
                return operation.getTables().contains(table) && operation.getType() == Type.COPY;
            }).findFirst();
        }

        public ImmutableList<Step> getSteps() {
            return ImmutableList.copyOf(steps);
        }
    }

    public static Builder builder(MigrationState migrationState) {
        return new Builder(migrationState);
    }

    private final ImmutableSet<Table> ghostTables;
    private final RefLog              refLog;
    private final ImmutableList<Step> steps;
    private final ImmutableSet<View>  views;

    public Plan(List<Step> steps, RefLog refLog, Set<Table> ghostTables, Set<View> views) {
        this.steps = ImmutableList.copyOf(steps);
        this.refLog = refLog;
        this.ghostTables = ImmutableSet.copyOf(ghostTables);
        this.views = ImmutableSet.copyOf(views);
    }

    @java.lang.Override
    
    public boolean equals(final java.lang.Object o) {
        if (o == this)
            return true;
        if (!(o instanceof Plan))
            return false;
        final Plan other = (Plan) o;
        if (!other.canEqual(this))
            return false;
        final java.lang.Object this$steps = this.getSteps();
        final java.lang.Object other$steps = other.getSteps();
        if (this$steps == null ? other$steps != null : !this$steps.equals(other$steps))
            return false;
        final java.lang.Object this$refLog = this.getRefLog();
        final java.lang.Object other$refLog = other.getRefLog();
        if (this$refLog == null ? other$refLog != null : !this$refLog.equals(other$refLog))
            return false;
        final java.lang.Object this$ghostTables = this.getGhostTables();
        final java.lang.Object other$ghostTables = other.getGhostTables();
        if (this$ghostTables == null ? other$ghostTables != null : !this$ghostTables.equals(other$ghostTables))
            return false;
        final java.lang.Object this$views = this.getViews();
        final java.lang.Object other$views = other.getViews();
        if (this$views == null ? other$views != null : !this$views.equals(other$views))
            return false;
        return true;
    }

    
    public ImmutableSet<Table> getGhostTables() {
        return this.ghostTables;
    }

    
    public RefLog getRefLog() {
        return this.refLog;
    }

    public ImmutableList<Step> getSteps() {
        return steps;
    }

    
    public ImmutableSet<View> getViews() {
        return this.views;
    }

    @java.lang.Override
    
    public int hashCode() {
        final int PRIME = 59;
        int result = 1;
        final java.lang.Object $steps = this.getSteps();
        result = result * PRIME + ($steps == null ? 43 : $steps.hashCode());
        final java.lang.Object $refLog = this.getRefLog();
        result = result * PRIME + ($refLog == null ? 43 : $refLog.hashCode());
        final java.lang.Object $ghostTables = this.getGhostTables();
        result = result * PRIME + ($ghostTables == null ? 43 : $ghostTables.hashCode());
        final java.lang.Object $views = this.getViews();
        result = result * PRIME + ($views == null ? 43 : $views.hashCode());
        return result;
    }

    public boolean isExecuted() {
        return steps.stream().allMatch(Step::isExecuted);
    }

    public Optional<Step> nextStep() {
        return steps.stream().filter(Step::canBeExecuted).findFirst();
    }

    @Override
    public String toString() {
        List<Step> toPrint = Lists.newArrayList(steps);
        StringBuilder builder = new StringBuilder();
        for (int i = 0; i < toPrint.size(); i++) {
            Step step = toPrint.get(i);
            Set<Step> dependencies = step.getDependencies();
            builder.append((i + 1) + ".\t");
            builder.append(step);
            if (!dependencies.isEmpty()) {
                builder.append(" depends on: ");
                builder.append(dependencies.stream()
                                           .map(toPrint::indexOf)
                                           .map(position -> position + 1)
                                           .collect(Collectors.toList()));
            }
            builder.append("\n");
        }
        return builder.toString();
    }

    
    protected boolean canEqual(final java.lang.Object other) {
        return other instanceof Plan;
    }
}
