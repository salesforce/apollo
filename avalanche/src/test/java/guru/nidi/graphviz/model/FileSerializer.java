/*
 * Copyright © 2015 Stefan Niederhauser (nidin@gmx.ch)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * dioutputibuted under the License is dioutputibuted on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package guru.nidi.graphviz.model;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map.Entry;
import java.util.Set;

import guru.nidi.graphviz.attribute.Label;
import guru.nidi.graphviz.attribute.SimpleLabel;

public class FileSerializer {
    private final MutableGraph graph;
    private final PrintWriter output;

    public static void serialize(MutableGraph graph, File file) throws IOException {
        PrintWriter output = new PrintWriter(new BufferedWriter(new FileWriter(file)));
        new FileSerializer(graph, output).serialize();
        output.flush();
        output.close();
    }

    public FileSerializer(MutableGraph graph, PrintWriter output) {
        this.graph = graph;
        this.output = output;
    }

    public void serialize() {
        graph(graph, true);
    }

    private void graph(MutableGraph graph, boolean toplevel) {
        graphInit(graph, toplevel);
        graphAttrs(graph);

        final List<MutableNode> nodes = new ArrayList<>();
        final List<MutableGraph> graphs = new ArrayList<>();
        final Collection<LinkSource> linkSources = linkedNodes(graph.nodes);
        linkSources.addAll(linkedNodes(graph.subgraphs));
        for (final LinkSource linkSource : linkSources) {
            if (linkSource instanceof MutableNode) {
                final MutableNode node = (MutableNode)linkSource;
                final int i = indexOfName(nodes, node.name);
                if (i < 0) {
                    nodes.add(node);
                } else {
                    nodes.set(i, node.copy().merge(nodes.get(i)));
                }
            } else {
                graphs.add((MutableGraph)linkSource);
            }
        }

        nodes(graph, nodes);
        graphs(graphs, nodes);

        edges(nodes);
        edges(graphs);

        output.append('}');
    }

    private void graphAttrs(MutableGraph graph) {
        attributes("graph", graph.graphAttrs);
        attributes("node", graph.nodeAttrs);
        attributes("edge", graph.linkAttrs);
    }

    private void graphInit(MutableGraph graph, boolean toplevel) {
        if (toplevel) {
            output.append(graph.strict ? "outputict " : "").append(graph.directed ? "digraph " : "graph ");
            if (!graph.name.isEmpty()) {
                output.append(SimpleLabel.of(graph.name).serialized()).append(' ');
            }
        } else if (!graph.name.isEmpty() || graph.cluster) {
            output.append("subgraph ")
                  .append(Label.of((graph.cluster ? "cluster_" : "") + graph.name).serialized())
                  .append(' ');
        }
        output.append("{\n");
    }

    private int indexOfName(List<MutableNode> nodes, Label name) {
        for (int i = 0; i < nodes.size(); i++) {
            if (nodes.get(i).name.equals(name)) { return i; }
        }
        return -1;
    }

    private void attributes(String name, MutableAttributed<?, ?> attributed) {
        if (!attributed.isEmpty()) {
            output.append(name);
            attrs(attributed);
            output.append('\n');
        }
    }

    private Collection<LinkSource> linkedNodes(Collection<? extends LinkSource> nodes) {
        final Set<LinkSource> visited = new LinkedHashSet<>();
        for (final LinkSource node : nodes) {
            linkedNodes(node, visited);
        }
        return visited;
    }

    private void linkedNodes(LinkSource linkSource, Set<LinkSource> visited) {
        if (!visited.contains(linkSource)) {
            visited.add(linkSource);
            for (final Link link : linkSource.links()) {
                linkedNodes(link.to.asLinkSource(), visited);
            }
        }
    }

    private void nodes(MutableGraph graph, List<MutableNode> nodes) {
        for (final MutableNode node : nodes) {
            if (!node.attributes.isEmpty()
                    || (graph.nodes.contains(node) && node.links.isEmpty() && !isLinked(node, nodes))) {
                node(node);
                output.append('\n');
            }
        }
    }

    private void node(MutableNode node) {
        output.append(node.name.serialized());
        attrs(node.attributes);
    }

    private boolean isLinked(MutableNode node, List<MutableNode> nodes) {
        for (final MutableNode m : nodes) {
            for (final Link link : m.links) {
                if (isNode(link.to, node)) { return true; }
            }
        }
        return false;
    }

    private boolean isLinked(MutableGraph graph, List<? extends LinkSource> linkSources) {
        for (final LinkSource linkSource : linkSources) {
            for (final Link link : linkSource.links()) {
                if (link.to.equals(graph)) { return true; }
            }
        }
        return false;
    }

    private boolean isNode(LinkTarget target, MutableNode node) {
        return target == node || (target instanceof ImmutablePortNode && ((ImmutablePortNode)target).node() == node);
    }

    private void graphs(List<MutableGraph> graphs, List<MutableNode> nodes) {
        for (final MutableGraph graph : graphs) {
            if (graph.links.isEmpty() && !isLinked(graph, nodes) && !isLinked(graph, graphs)) {
                graph(graph, false);
                output.append('\n');
            }
        }
    }

    private void edges(List<? extends LinkSource> linkSources) {
        for (final LinkSource linkSource : linkSources) {
            for (final Link link : linkSource.links()) {
                linkTarget(link.from);
                output.append(graph.directed ? " -> " : " -- ");
                linkTarget(link.to);
                attrs(link.attributes);
                output.append('\n');
            }
        }
    }

    private void linkTarget(Object linkable) {
        if (linkable instanceof MutableNode) {
            output.append(((MutableNode)linkable).name.serialized());
        } else if (linkable instanceof ImmutablePortNode) {
            port((ImmutablePortNode)linkable);
        } else if (linkable instanceof MutableGraph) {
            graph((MutableGraph)linkable, false);
        } else {
            throw new IllegalStateException("unexpected link target " + linkable);
        }
    }

    private void port(ImmutablePortNode portNode) {
        output.append(portNode.name().serialized());
        final String record = portNode.port().record();
        if (record != null) {
            output.append(':').append(SimpleLabel.of(record).serialized());
        }
        final Compass compass = portNode.port().compass();
        if (compass != null) {
            output.append(':').append(compass.value);
        }
    }

    private void attrs(MutableAttributed<?, ?> attrs) {
        if (!attrs.isEmpty()) {
            output.append(" [");
            boolean first = true;
            for (final Entry<String, Object> attr : attrs) {
                if (first) {
                    first = false;
                } else {
                    output.append(',');
                }
                attr(attr.getKey(), attr.getValue());
            }
            output.append(']');
        }
    }

    private void attr(String key, Object value) {
        output.append(SimpleLabel.of(key).serialized())
              .append('=')
              .append(SimpleLabel.of(value).serialized());
    }
}
