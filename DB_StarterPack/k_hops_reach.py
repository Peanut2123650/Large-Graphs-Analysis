#!/usr/bin/env python3
"""
k_hops_reach.py

Compute k-hop coverage (number or fraction of nodes reachable within k hops) for nodes in a graph.

Outputs: khop_results.csv with columns: node, khop_count, khop_fraction

Features:
- Uses adjacency dict for speed
- Works with string node IDs
- Progress bar via tqdm (optional)
- Optionally compute only for top candidates (by degree or by pagerank) to save time
"""

import os
import argparse
import pandas as pd
import networkx as nx
from collections import deque
from tqdm import tqdm

def build_graph_from_edges(edges_csv, undirected=True, edge_type_col='type', keep_type='friend'):
    df = pd.read_csv(edges_csv, dtype=str)
    # filter by edge type if present
    if edge_type_col in df.columns:
        df = df[df[edge_type_col].fillna(keep_type) == keep_type]
    if undirected:
        G = nx.from_pandas_edgelist(df, source='src', target='dst', create_using=nx.Graph())
    else:
        G = nx.from_pandas_edgelist(df, source='src', target='dst', create_using=nx.DiGraph())
    return G

def adjacency_dict(G):
    """Return dict node -> set(neighbors) for fast access."""
    return {n: set(G.neighbors(n)) for n in G.nodes()}

def khop_coverage_count(adj, source, k):
    """Return number of unique nodes reachable within k hops (including source)."""
    if k <= 0:
        return 1
    seen = {source}
    frontier = {source}
    for _ in range(k):
        # build next frontier as union of neighbors of current frontier
        nxt = set()
        for u in frontier:
            nxt |= adj.get(u, set())
        nxt -= seen
        if not nxt:
            break
        seen |= nxt
        frontier = nxt
    return len(seen)

def compute_khop_for_list(adj, nodes, k, show_progress=True):
    results = {}
    iterator = nodes
    if show_progress:
        iterator = tqdm(nodes, desc=f"{k}-hop")
    total_nodes = len(adj)
    for u in iterator:
        cnt = khop_coverage_count(adj, u, k)
        results[u] = cnt
    return results

def main():
    p = argparse.ArgumentParser()
    p.add_argument("--edges", required=True, help="Path to edges.csv")
    p.add_argument("--k", type=int, default=2, help="k hops")
    p.add_argument("--out", default="khop_results.csv", help="Output CSV file")
    p.add_argument("--fraction", action="store_true", help="Also output coverage fraction")
    p.add_argument("--candidates", type=int, default=0,
                   help="If >0, compute k-hop only for top N candidates (by 'degree' or 'pagerank') to save time")
    p.add_argument("--candidates_by", choices=['degree','pagerank'], default='degree',
                   help="When using --candidates, how to pick top nodes")
    p.add_argument("--no_progress", action="store_true", help="Disable tqdm progress bars")
    args = p.parse_args()

    print("Loading graph from:", args.edges)
    G = build_graph_from_edges(args.edges, undirected=True)
    print("Graph loaded: nodes =", G.number_of_nodes(), "edges =", G.number_of_edges())

    # adjacency dict for speed
    adj = adjacency_dict(G)
    nodes_all = list(G.nodes())
    total_nodes = len(nodes_all)

    # decide node list to compute
    if args.candidates and args.candidates > 0:
        n_cand = min(args.candidates, total_nodes)
        if args.candidates_by == 'degree':
            deg = dict(G.degree())
            top_nodes = sorted(deg.items(), key=lambda x: x[1], reverse=True)[:n_cand]
            node_list = [t[0] for t in top_nodes]
        else:
            # pagerank fallback - compute pagerank (fast-ish)
            pr = nx.pagerank(G, weight='weight')
            top_nodes = sorted(pr.items(), key=lambda x: x[1], reverse=True)[:n_cand]
            node_list = [t[0] for t in top_nodes]
        print(f"Computing k-hop for top {len(node_list)} candidates by {args.candidates_by}")
    else:
        node_list = nodes_all
        print(f"Computing k-hop for all {len(node_list)} nodes")

    show_progress = not args.no_progress

    khop_counts = compute_khop_for_list(adj, node_list, args.k, show_progress=show_progress)

    # prepare output rows for all nodes (fill zeros for those we skipped if using candidates mode)
    rows = []
    for n in nodes_all:
        cnt = khop_counts.get(n, 0)
        if args.fraction:
            frac = cnt / total_nodes if total_nodes > 0 else 0.0
            rows.append((n, cnt, frac))
        else:
            rows.append((n, cnt))

    # write CSV
    if args.fraction:
        df_out = pd.DataFrame(rows, columns=['node','khop_count','khop_fraction'])
    else:
        df_out = pd.DataFrame(rows, columns=['node','khop_count'])
    df_out.to_csv(args.out, index=False)
    print("Wrote", args.out)

if __name__ == "__main__":
    main()
