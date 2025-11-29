#!/usr/bin/env python3
"""
community_summary_with_khop_pct.py

Produces:
 - Number of communities (Louvain)
 - Modularity score
 - Community size distribution CSV
 - Leader per community CSV (selected by highest influence score) WITH:
     - leader_reach_k2 (count)
     - leader_reach_k2_pct (percentage of community reachable within 2 hops)
 - Console summary (largest/smallest/average sizes + leaders snippet)

No visualization included.
"""
import math
import pandas as pd
import networkx as nx
from collections import defaultdict
import community as community_louvain   # python-louvain
import os

# ---------------- CONFIG: update if your files are elsewhere ----------------
EDGES_PATH = r'C:\Users\p2123\Desktop\COLLEGE\PROJECT_3rdYear\Social_Network_Project\data\edges.csv'
INFLUENCE_PATH = r'C:\Users\p2123\Desktop\COLLEGE\PROJECT_3rdYear\Social_Network_Project\data\influence_scores_formula.csv'

OUT_COMM_SIZES = r'C:\Users\p2123\Desktop\COLLEGE\PROJECT_3rdYear\Social_Network_Project\data\community_size_distribution.csv'
OUT_LEADERS = r'C:\Users\p2123\Desktop\COLLEGE\PROJECT_3rdYear\Social_Network_Project\data\per_community_leaders.csv'

# ---------------- helpers ----------------
def load_graph_from_edges(path):
    if not os.path.exists(path):
        raise FileNotFoundError(f"Edges file not found: {path}")
    df = pd.read_csv(path)
    cols_low = [c.lower() for c in df.columns]
    if 'source' in cols_low and 'target' in cols_low:
        sc = df.columns[cols_low.index('source')]
        tc = df.columns[cols_low.index('target')]
    elif 'src' in cols_low and 'dst' in cols_low:
        sc = df.columns[cols_low.index('src')]
        tc = df.columns[cols_low.index('dst')]
    elif 'u' in cols_low and 'v' in cols_low:
        sc = df.columns[cols_low.index('u')]
        tc = df.columns[cols_low.index('v')]
    else:
        sc, tc = df.columns[0], df.columns[1]

    edges = []
    for _, r in df.iterrows():
        u = r[sc]; v = r[tc]
        # normalize ids to strings (attempt integer conversion first)
        try:
            u = str(int(u))
            v = str(int(v))
        except Exception:
            u = str(u)
            v = str(v)
        edges.append((u, v))

    G = nx.Graph()
    G.add_edges_from(edges)
    return G

def read_influence_map(path):
    if not os.path.exists(path):
        raise FileNotFoundError(f"Influence file not found: {path}")
    inf = pd.read_csv(path)
    node_col = "node" if "node" in inf.columns else inf.columns[0]

    # candidate names in order of preference
    candidates = ['Influence_I', 'Influence', 'A', 'A_n', 'influence_i', 'influence', 'InfluenceScore', 'score']
    score_col = None
    for c in candidates:
        if c in inf.columns:
            score_col = c
            break
    # fallback to second column if none matched
    if score_col is None:
        if len(inf.columns) >= 2:
            score_col = inf.columns[1]
        else:
            score_col = inf.columns[0]

    # normalize node ids to strings (try int then str)
    try:
        inf[node_col] = inf[node_col].astype(int).astype(str)
    except Exception:
        inf[node_col] = inf[node_col].astype(str)

    # build a mapping node -> score (float)
    score_map = {}
    for _, r in inf.iterrows():
        n = str(r[node_col])
        try:
            score_map[n] = float(r[score_col])
        except Exception:
            # if can't cast, set NaN
            score_map[n] = float('nan')

    return inf, node_col, score_col, score_map

def compute_khop_reach_within(G, members, leader, k=2):
    """
    Compute k-hop reach strictly within the community.
    BFS is performed only on the induced subgraph of 'members'.
    """
    # Create induced subgraph of the community
    G_sub = G.subgraph(members)

    if leader not in G_sub:
        return 0

    # BFS restricted to nodes inside the community only
    lengths = nx.single_source_shortest_path_length(G_sub, leader, cutoff=k)

    return len(lengths)


# ---------------- main ----------------
def main():
    print("Loading graph from:", EDGES_PATH)
    G = load_graph_from_edges(EDGES_PATH)
    print("Graph loaded: nodes =", G.number_of_nodes(), "edges =", G.number_of_edges())

    print("Loading influence file from:", INFLUENCE_PATH)
    inf_df, node_col, score_col, score_map = read_influence_map(INFLUENCE_PATH)
    print("Using influence column:", score_col)

    print("Running Louvain community detection (full graph)...")
    partition = community_louvain.best_partition(G)
    num_comms = len(set(partition.values()))
    print("Number of communities detected:", num_comms)

    modularity = community_louvain.modularity(partition, G)
    print("Modularity score:", modularity)

    # build community -> members mapping (as string ids)
    comm_map = defaultdict(list)
    for node, comm in partition.items():
        comm_map[comm].append(str(node))

    # community size distribution DF
    comm_rows = []
    for comm, members in comm_map.items():
        comm_rows.append({'community': comm, 'size': len(members)})
    comm_sizes_df = pd.DataFrame(comm_rows).sort_values('size', ascending=False).reset_index(drop=True)
    comm_sizes_df.to_csv(OUT_COMM_SIZES, index=False)
    print("Saved community size distribution to:", OUT_COMM_SIZES)

    # largest, smallest, average
    sizes = comm_sizes_df['size'].values
    largest = int(sizes.max())
    smallest = int(sizes.min())
    average = float(sizes.mean())

    # select leader (max influence) for each community and compute k-hop reach + pct
    leaders_rows = []
    for comm, members in sorted(comm_map.items(), key=lambda x: x[0]):  # sort by community id for stability
        best_node = None
        best_score = float('-inf')
        # choose the highest valid numeric score (skip NaN)
        for m in members:
            s = score_map.get(str(m), None)
            if s is None or (isinstance(s, float) and (math.isnan(s))):
                continue
            try:
                sf = float(s)
            except:
                continue
            if sf > best_score:
                best_score = sf
                best_node = str(m)
        # If no member had a valid score, fall back to highest degree within community
        if best_node is None:
            # fallback winner by degree (prefer nodes present in G)
            try:
                best_node = max(members, key=lambda n: G.degree(n) if n in G else -1)
                best_score = score_map.get(best_node, float('nan'))
            except Exception:
                best_node = members[0]
                best_score = score_map.get(best_node, float('nan'))

        # Compute leader reach within community using k = 2
        reach_k2 = compute_khop_reach_within(G, members, best_node, k=2)


        # Compute percentage (defensive against division by zero)
        comm_size = len(members)
        if comm_size > 0:
            reach_pct = round((reach_k2 / comm_size) * 100.0, 2)
        else:
            reach_pct = 0.0

        leaders_rows.append({
            'community': comm,
            'leader_node': best_node,
            score_col: (round(float(best_score), 6) if (best_score is not None and not (isinstance(best_score, float) and math.isnan(best_score))) else ''),
            'community_size': comm_size,
            'leader_reach_k2': reach_k2,
            'leader_reach_k2_pct': reach_pct
        })

    leaders_df = pd.DataFrame(leaders_rows).sort_values('community').reset_index(drop=True)
    leaders_df.to_csv(OUT_LEADERS, index=False)
    print("Saved per-community leaders to:", OUT_LEADERS)

    # print summary similar to your image
    print("\n=== Louvain Algorithm Output ===")
    print(f"Number of Communities: {num_comms}")
    print(f"Modularity Score: {round(modularity, 4)}")
    print("Community Size Distribution:")
    print(f"  Largest community: {largest} nodes ({round(100.0 * largest / G.number_of_nodes(), 1)}%)")
    print(f"  Smallest community: {smallest} nodes ({round(100.0 * smallest / G.number_of_nodes(), 1)}%)")
    print(f"  Average size: {round(average, 1)} nodes\n")

    print("Leader Nodes per Community (first 20 rows):")
    # include leader_reach_k2 and percentage in printed snippet
    cols_to_show = ['community', 'leader_node', score_col, 'community_size', 'leader_reach_k2', 'leader_reach_k2_pct']
    cols_to_show = [c for c in cols_to_show if c in leaders_df.columns]
    to_print = leaders_df[cols_to_show].head(20)
    print(to_print.to_string(index=False))

if __name__ == "__main__":
    main()
