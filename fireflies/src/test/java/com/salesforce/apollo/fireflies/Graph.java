/*
 * Copyright (c) 2019, salesforce.com, inc.
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * For full license text, see the LICENSE file in the repo root or https://opensource.org/licenses/BSD-3-Clause
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

        var notVisited = visited.entrySet().stream().filter(e -> !e.getValue()).map(e -> e.getKey()).toList();
        if (notVisited.size() != 0) {
            System.out.println("Not visited: " + notVisited);
            return false;
        }

        Graph<T> gr = getTranspose();

        for (T m : adj.keySet())
            visited.put(m, false);

        gr.dfs(startingMember, visited);

        notVisited = visited.entrySet().stream().filter(e -> !e.getValue()).map(e -> e.getKey()).toList();
        if (notVisited.size() == 0) {
            return true;
        }
        System.out.println("Not visited: " + notVisited);
        return false;
    }

    private void dfs(T v, Map<T, Boolean> visited) {
        visited.put(v, true);

        final var list = adj.get(v);
        if (list == null) {
            return;
        }
        for (T n : list) {
            if (!visited.computeIfAbsent(n, k -> false))
                dfs(n, visited);
        }
    }
}
