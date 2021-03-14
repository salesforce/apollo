
package io.quantumdb.core.backends.planner;

import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import io.quantumdb.core.backends.planner.Operation.Type;
import io.quantumdb.core.schema.definitions.Table;

public class Step {
    public static Step addNull(Set<Table> tables, Step... dependentOn) {
        Step step = new Step(new Operation(tables, Type.ADD_NULL));
        Arrays.stream(dependentOn).forEach(step::makeDependentOn);
        return step;
    }

    public static Step copy(Table table, LinkedHashSet<String> columns, Step... dependentOn) {
        Step step = new Step(new Operation(table, columns, Type.COPY));
        Arrays.stream(dependentOn).forEach(step::makeDependentOn);
        return step;
    }

    public static Step dropNull(Set<Table> tables, Step... dependentOn) {
        Step step = new Step(new Operation(tables, Type.DROP_NULL));
        Arrays.stream(dependentOn).forEach(step::makeDependentOn);
        return step;
    }

    private final Set<Step>     dependsOn;
    private final AtomicBoolean executed;
    private final Operation     operation;

    Step(Operation operation) {
        this.operation = operation;
        this.executed = new AtomicBoolean(false);
        this.dependsOn = Sets.newHashSet();
    }

    public boolean canBeExecuted() {
        return !isExecuted() && dependsOn.stream().allMatch(Step::isExecuted);
    }

    @Override
    public boolean equals(Object other) {
        if (other instanceof Step) {
            Step step = (Step) other;
            return new EqualsBuilder().append(operation, step.operation).isEquals();
        }
        return false;
    }

    public Set<Step> getDependencies() {
        return Sets.newHashSet(dependsOn);
    }

    
    public Operation getOperation() {
        return this.operation;
    }

    public Set<Step> getTransitiveDependencies() {
        Set<Step> dependencies = Sets.newHashSet();
        List<Step> queue = Lists.newLinkedList(dependsOn);
        while (!queue.isEmpty()) {
            Step removed = queue.remove(0);
            dependencies.add(removed);
            removed.getDependencies().forEach(dependency -> {
                if (!queue.contains(dependency) && !dependencies.contains(dependency)) {
                    queue.add(dependency);
                }
            });
        }
        return dependencies;
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder().append(operation).toHashCode();
    }

    public boolean isExecuted() {
        return executed.get();
    }

    public void makeDependentOn(Step other) {
        Preconditions.checkArgument(!other.getTransitiveDependencies().contains(this), "This would cause a cycle!");
        dependsOn.add(other);
    }

    public void markAsExecuted() {
        if (!executed.compareAndSet(false, true)) {
            throw new IllegalStateException("Step was already marked as executed!");
        }
    }

    public void removeDependencyOn(Step other) {
        dependsOn.remove(other);
    }

    @Override
    public String toString() {
        return operation.toString();
    }
}
