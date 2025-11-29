#!/usr/bin/env python3

import os
import math
import pandas as pd
import networkx as nx
import matplotlib.pyplot as plt
import numpy as np
from collections import defaultdict
from community import community_louvain
from sklearn.neighbors import NearestNeighbors

# ---------------- CONFIG ----------------
EDGES_PATH = r'C:\Users\p2123\Desktop\COLLEGE\PROJECT_3rdYear\Social_Network_Project\data\edges.csv'
INFLUENCE_PATH = r'C:\Users\p2123\Desktop\COLLEGE\PROJECT_3rdYear\Social_Network_Project\data\influence_scores_formula.csv'
OUT_IMG = r'C:\Users\p2123\Desktop\COLLEGE\PROJECT_3rdYear\Social_Network_Project\data\top200_leaders_visualization.png'
OUT_LEADERS_CSV = r'C:\Users\p2123\Desktop\COLLEGE\PROJECT_3rdYear\Social_Network_Project\data\corrected_per_community_leaders.csv'
OUT_SAMPLE_CSV = r'C:\Users\p2123\Desktop\COLLEGE\PROJECT_3rdYear\Social_Network_Project\data\sampled_nodes_for_visualization.csv'

TARGET_SAMPLE = 200
RANDOM_SEED = 42

# ----------------------------------------
def load_edges_build_graph(edges_path):
    df = pd.read_csv(edges_path)
    cols = [c.lower() for c in df.columns]

    if 'src' in cols and 'dst' in cols:
        src_col = df.columns[cols.index('src')]
        dst_col = df.columns[cols.index('dst')]
    elif 'source' in cols and 'target' in cols:
        src_col = df.columns[cols.index('source')]
        dst_col = df.columns[cols.index('target')]
    else:
        src_col, dst_col = df.columns[0], df.columns[1]

    edges = []
    for _, row in df.iterrows():
        u, v = row[src_col], row[dst_col]
        try:
            u = str(int(u)); v = str(int(v))
        except:
            u, v = str(u), str(v)
        edges.append((u, v))

    G = nx.Graph()
    G.add_edges_from(edges)
    return G


def read_influence(inf_path):
    inf = pd.read_csv(inf_path)
    node_col = "node" if "node" in inf.columns else inf.columns[0]

    candidate_names = ['Influence_I','Influence','A','A_n','influence_i','influence']
    score_col = None
    for name in candidate_names:
        if name in inf.columns:
            score_col = name; break
    if score_col is None:
        score_col = inf.columns[1]

    inf[node_col] = inf[node_col].astype(int).astype(str)
    inf = inf.sort_values(score_col, ascending=False).reset_index(drop=True)
    score_map = inf.set_index(node_col)[score_col].to_dict()

    return inf, node_col, score_col, score_map


def mean_nearest_dist(pos):
    pts = np.array(list(pos.values()))
    if pts.shape[0] < 2:
        return 0.0
    nbrs = NearestNeighbors(n_neighbors=2).fit(pts)
    dist, _ = nbrs.kneighbors(pts)
    return float(dist[:,1].mean())


# ---------------- MAIN ----------------
def main():
    print("Loading graph...")
    G_full = load_edges_build_graph(EDGES_PATH)

    print("Loading influence...")
    inf_df, node_col, score_col, scores_map = read_influence(INFLUENCE_PATH)

    print("Running Louvain...")
    partition_full = community_louvain.best_partition(G_full)

    comm_members = defaultdict(list)
    for n, c in partition_full.items():
        comm_members[c].append(n)

    print("Selecting leaders...")
    leaders = []
    for c, members in comm_members.items():
        best_node, best_score = None, -1
        for m in members:
            s = scores_map.get(str(m))
            if s is None: continue
            if s > best_score:
                best_score = s
                best_node = str(m)
        if best_node:
            leaders.append(best_node)

    leaders = sorted(set(leaders), key=lambda x: int(x))
    print("Leaders:", len(leaders))

    top_nodes = inf_df[node_col].astype(str).tolist()
    sample = []

    for L in leaders:
        if L not in sample:
            sample.append(L)

    for n in top_nodes:
        if len(sample) >= TARGET_SAMPLE: break
        if n not in sample:
            sample.append(n)

    if len(sample) < TARGET_SAMPLE:
        for n in G_full.nodes():
            n = str(n)
            if n not in sample:
                sample.append(n)
            if len(sample) >= TARGET_SAMPLE:
                break

    print("Final sample:", len(sample))

    G_sub = G_full.subgraph(sample).copy()

    print("Selecting layout...")
    k_auto = 2.0 / math.sqrt(max(1, G_sub.number_of_nodes()))
    pos_spring = nx.spring_layout(G_sub, seed=42, k=k_auto, iterations=800, scale=3.0)
    score_spring = mean_nearest_dist(pos_spring)

    try:
        pos_kk = nx.kamada_kawai_layout(G_sub)
        score_kk = mean_nearest_dist(pos_kk)
    except:
        pos_kk, score_kk = None, -1

    pos = pos_kk if score_kk > score_spring else pos_spring

    # ---------------- DRAWING ----------------
    plt.figure(figsize=(14, 10))
    ax = plt.gca()

    # ❗ DARKER edges (~30% darker)
    nx.draw_networkx_edges(
        G_sub, pos, ax=ax,
        alpha=0.32,  # Increased transparency for darker edges
        edge_color="#8a8a8a",  # Darker gray for visibility
        width=1.0  # Slightly thicker edges
    )

    # ❗ DARKER background nodes
    non_leaders = [n for n in G_sub.nodes() if n not in leaders]
    nx.draw_networkx_nodes(
        G_sub, pos,
        nodelist=non_leaders,
        node_size=45,
        node_color="#9fc7e8",
        alpha=1.0,
        ax=ax
    )

    # halos
    hx = [pos[n][0] for n in leaders if n in pos]
    hy = [pos[n][1] for n in leaders if n in pos]
    for size, alpha in [(900, .10),(600,.06),(300,.03)]:
        ax.scatter(hx, hy, s=size, c="#ffd54f", alpha=alpha, linewidths=0)

    # leaders
    leader_draw = [n for n in leaders if n in pos]
    leader_sizes = []
    for n in leader_draw:
        v = float(scores_map.get(n, 0))
        leader_sizes.append(420 * (0.8 + 0.6*v))

    nx.draw_networkx_nodes(
        G_sub, pos,
        nodelist=leader_draw,
        node_size=leader_sizes,
        node_color="#d73027",
        edgecolors="black",
        linewidths=1.6,
        ax=ax
    )

    # labels
    ymin = min(p[1] for p in pos.values())
    ymax = max(p[1] for p in pos.values())
    y_offset = (ymax - ymin)*0.03

    for n in leader_draw:
        x, y = pos[n]
        ax.text(
            x, y + y_offset, str(n),
            fontsize=10, fontweight='bold',
            ha='center', va='bottom',
            bbox=dict(facecolor='white',alpha=0.6,edgecolor='none',pad=1)
        )

    ax.set_axis_off()
    plt.title("Top sample (leaders highlighted) — leaders computed on full graph")
    plt.tight_layout()
    plt.savefig(OUT_IMG, dpi=300, bbox_inches='tight')
    plt.close()

    pd.DataFrame({"community_leader_node": leaders}).to_csv(OUT_LEADERS_CSV, index=False)
    pd.DataFrame({"node": sample}).to_csv(OUT_SAMPLE_CSV, index=False)


if __name__ == "__main__":
    main()
