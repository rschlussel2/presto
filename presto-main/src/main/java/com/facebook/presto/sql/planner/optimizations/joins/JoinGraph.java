/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.facebook.presto.sql.planner.optimizations.joins;

import com.facebook.presto.sql.planner.Symbol;
import com.facebook.presto.sql.planner.plan.FilterNode;
import com.facebook.presto.sql.planner.plan.JoinNode;
import com.facebook.presto.sql.planner.plan.PlanNode;
import com.facebook.presto.sql.planner.plan.PlanNodeId;
import com.facebook.presto.sql.planner.plan.PlanVisitor;
import com.facebook.presto.sql.tree.Expression;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.Multimap;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.facebook.presto.sql.planner.plan.JoinNode.Type.INNER;
import static com.google.common.base.Preconditions.checkState;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

/**
 * JoinGraph represents sequence of Joins, where nodes in the graph
 * are PlanNodes that are being joined and edges are all equality join
 * conditions between pair of nodes.
 */
public class JoinGraph
{
    private final List<Expression> filters;
    private final List<PlanNode> nodes; // nodes in order of they appearance in plan
    private final Multimap<PlanNode, Edge> edges;
    private final PlanNodeId rootId;

    public static List<JoinGraph> buildFrom(PlanNode plan)
    {
        Context context = new Context();
        JoinGraph graph = plan.accept(new Builder(), context);
        if (graph.size() > 1) {
            context.addSubGraph(graph);
        }
        return context.getGraphs();
    }

    public JoinGraph(PlanNode node)
    {
        this(ImmutableList.of(node), ImmutableMultimap.of(), node.getId(), ImmutableList.of());
    }

    public JoinGraph(List<PlanNode> nodes, Multimap<PlanNode, Edge> edges, PlanNodeId rootId, List<Expression> filters)
    {
        this.nodes = nodes;
        this.edges = edges;
        this.rootId = rootId;
        this.filters = filters;
    }

    public JoinGraph withFilter(Expression expression)
    {
        ImmutableList.Builder<Expression> filters = ImmutableList.builder();
        filters.addAll(this.filters);
        filters.add(expression);

        return new JoinGraph(nodes, edges, rootId, filters.build());
    }

    public List<Expression> getFilters()
    {
        return filters;
    }

    public PlanNodeId getRootId()
    {
        return rootId;
    }

    public JoinGraph withRootId(PlanNodeId rootId)
    {
        return new JoinGraph(nodes, edges, rootId, filters);
    }

    public boolean isEmpty()
    {
        return nodes.isEmpty();
    }

    public int size()
    {
        return nodes.size();
    }

    public PlanNode getNode(int index)
    {
        return nodes.get(index);
    }

    public Collection<Edge> getEdges(PlanNode node)
    {
        return edges.get(node);
    }

    @Override
    public String toString()
    {
        StringBuilder builder = new StringBuilder();

        for (PlanNode nodeFrom : nodes) {
            builder.append(nodeFrom.getId())
                    .append(" = ")
                    .append(nodeFrom.toString())
                    .append("\n");
        }
        for (PlanNode nodeFrom : nodes) {
            builder.append(nodeFrom.getId())
                    .append(":");
            for (Edge nodeTo : edges.get(nodeFrom)) {
                builder.append(" ").append(nodeTo.getTargetNode().getId());
            }
            builder.append("\n");
        }

        return builder.toString();
    }

    private JoinGraph joinWith(JoinGraph other, List<JoinNode.EquiJoinClause> joinClauses, Context context, PlanNodeId rootId)
    {
        for (PlanNode node : other.nodes) {
            checkState(!edges.containsKey(node), format("Node [%s] appeared in two JoinGraphs", node));
        }

        ImmutableList.Builder<PlanNode> joinedNodes = ImmutableList.builder();
        ImmutableMultimap.Builder<PlanNode, Edge> joinedEdges = ImmutableMultimap.builder();

        joinedNodes.addAll(this.nodes);
        joinedNodes.addAll(other.nodes);
        joinedEdges.putAll(this.edges);
        joinedEdges.putAll(other.edges);

        for (JoinNode.EquiJoinClause edge : joinClauses) {
            checkState(context.containsSymbol(edge.getLeft()));
            checkState(context.containsSymbol(edge.getRight()));

            PlanNode left = context.getSymbolSource(edge.getLeft());
            PlanNode right = context.getSymbolSource(edge.getRight());
            Symbol symbol1 = edge.getLeft();
            Symbol symbol2 = edge.getRight();

            joinedEdges.put(left, new Edge(right, symbol1, symbol2));
            joinedEdges.put(right, new Edge(left, symbol2, symbol1));
        }

        return new JoinGraph(joinedNodes.build(), joinedEdges.build(), rootId, other.filters);
    }

    private static class Builder
            extends PlanVisitor<Context, JoinGraph>
    {
        @Override
        protected JoinGraph visitPlan(PlanNode node, Context context)
        {
            for (PlanNode child : node.getSources()) {
                JoinGraph graph = child.accept(this, context);
                if (graph.size() < 2) {
                    continue;
                }
                context.addSubGraph(graph.withRootId(child.getId()));
            }

            for (Symbol symbol : node.getOutputSymbols()) {
                context.setSymbolSource(symbol, node);
            }
            return new JoinGraph(node);
        }

        @Override
        public JoinGraph visitFilter(FilterNode node, Context context)
        {
            JoinGraph graph = node.getSource().accept(this, context);
            return graph.withFilter(node.getPredicate());
        }

        @Override
        public JoinGraph visitJoin(JoinNode node, Context context)
        {
            //TODO: add support for non inner joins
            if (node.getType() != INNER) {
                return visitPlan(node, context);
            }

            JoinGraph left = node.getLeft().accept(this, context);
            JoinGraph right = node.getRight().accept(this, context);

            JoinGraph graph = left.joinWith(right, node.getCriteria(), context, node.getId());

            if (node.getFilter().isPresent()) {
                return graph.withFilter(node.getFilter().get());
            }
            return graph;
        }
    }

    public static class Edge
    {
        private final PlanNode targetNode;
        private final Symbol sourceSymbol;
        private final Symbol targetSymbol;

        public Edge(PlanNode targetNode, Symbol sourceSymbol, Symbol targetSymbol)
        {
            this.targetNode = requireNonNull(targetNode, "targetNode is null");
            this.sourceSymbol = requireNonNull(sourceSymbol, "sourceSymbol is null");
            this.targetSymbol = requireNonNull(targetSymbol, "targetSymbol is null");
        }

        public PlanNode getTargetNode()
        {
            return targetNode;
        }

        public Symbol getSourceSymbol()
        {
            return sourceSymbol;
        }

        public Symbol getTargetSymbol()
        {
            return targetSymbol;
        }
    }

    private static class Context
    {
        private Map<Symbol, PlanNode> symbolSources = new HashMap<>();
        private List<JoinGraph> joinGraphs = new ArrayList<>();

        public void setSymbolSource(Symbol symbol, PlanNode node)
        {
            symbolSources.put(symbol, node);
        }

        public void addSubGraph(JoinGraph graph)
        {
            joinGraphs.add(graph);
        }

        public boolean containsSymbol(Symbol symbol)
        {
            return symbolSources.containsKey(symbol);
        }

        public PlanNode getSymbolSource(Symbol symbol)
        {
            checkState(containsSymbol(symbol));
            return symbolSources.get(symbol);
        }

        public List<JoinGraph> getGraphs()
        {
            return joinGraphs;
        }
    }
}
