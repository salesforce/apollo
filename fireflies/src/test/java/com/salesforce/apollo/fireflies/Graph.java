/*
 * Copyright 2019, salesforce.com
 * All Rights Reserved
 * Company Confidential
 */
package com.salesforce.apollo.fireflies;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Graph<T> {
    private final Map<T, List<T>> adj = new HashMap<>();

    public void addEdge(T v, T w) {
        adj.computeIfAbsent(v, k -> new ArrayList<>()).add(w);
    }

    public Graph<T> getTranspose() {
        Graph<T> g = new Graph<T>();
        for (T v : adj.keySet()) {
            for (T i : adj.get(v))
                g.addEdge(v, i);
        }
        return g;
    }

    public boolean isSC() {
        Map<T, Boolean> visited = new HashMap<>();
        for (T i : adj.keySet())
            visited.put(i, false);

        T startingMember = adj.keySet().iterator().next();
        dfs(startingMember, visited);

        if (visited.values().stream().filter(e -> !e).count() != 0) {
            return false;
        }

        Graph<T> gr = getTranspose();

        for (T m : adj.keySet())
            visited.put(m, false);

        gr.dfs(startingMember, visited);

        return visited.values().stream().filter(e -> !e).count() == 0;
    }

    private void dfs(T v, Map<T, Boolean> visited) {
        visited.put(v, true);

        for (T n : adj.get(v)) {
            if (!visited.computeIfAbsent(n, k -> false))
                dfs(n, visited);
        }
    }
}
