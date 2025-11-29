# select_leader_deputy.py
"""
Detect communities with Louvain, compute centralities + structural homophily,
normalize scores and select Leader and Deputy per community.

Saves:
 - per_community_nodes_features.csv
 - per_community_communities.csv
 - per_community_leaders.csv
 - per_community_leaders_only_visualization.png
"""

import os
import random
import math
from collections import defaultdict

import matplotlib.pyplot as plt
from matplotlib.patches import Circle
import networkx as nx
import pandas as pd
import community as community_louvain   # pip install python-louvain
from sklearn.preprocessing import MinMaxScaler

# Config
EDGES_CSV = "edges.csv"       # default filename (script will search for it)
USERS_CSV = "users.csv"       # optional; used only if exists
OUT_PREFIX = "per_community"
RANDOM_SEED = 42
SAMPLE_SIZE = 250             # If graph larger than ~300, we will sample down to this many nodes
MIN_PER_COMMUNITY = 2         # ensure at least this many nodes per community during sampling

random.seed(RANDOM_SEED)


def find_file_in_tree(filename, search_roots=None):
    """
    Search for filename in the current directory and common project folders.
    Returns the first matching full path or None.
    """
    if search_roots is None:
        cwd = os.getcwd()
        # include cwd and one common project folder names
        search_roots = [cwd,
                        os.path.join(cwd, "DB_StarterPack"),
                        os.path.join(cwd, "DB_StarterPack", "lib"),
                        os.path.join(cwd, ".."), os.path.join(cwd, "..", "DB_StarterPack")]
    for root in search_roots:
        if not root:
            continue
        for dirpath, _dirs, files in os.walk(root):
            if filename in files:
                return os.path.join(dirpath, filename)
    return None


def load_graph(edges_csv_path=None):
    if edges_csv_path is None:
        # try to find edges.csv
        found = find_file_in_tree(EDGES_CSV)
        if found:
            edges_csv_path = found
            print(f"Found edges file at: {edges_csv_path}")
        else:
            raise FileNotFoundError(f"Could not find {EDGES_CSV} in project tree. Looked under working directory and common project folders.")
    df = pd.read_csv(edges_csv_path)
    # detect column names: prefer source,target
    if {"source", "target"}.issubset(df.columns):
        edges = df[["source", "target"]].values.tolist()
    elif {"u", "v"}.issubset(df.columns):
        edges = df[["u", "v"]].values.tolist()
    else:
        # fallback: use first two columns
        edges = df.iloc[:, :2].values.tolist()
    G = nx.Graph()
    G.add_edges_from(edges)
    return G, edges_csv_path


def compute_features(G, users_df=None):
    deg = dict(G.degree())
    pr = nx.pagerank(G)
    try:
        eig = nx.eigenvector_centrality_numpy(G)
    except Exception:
        eig = {n: 0.0 for n in G.nodes()}
    # betweenness centrality (may be slow for large graphs)
    between = nx.betweenness_centrality(G)

    # homophily placeholder (will be overwritten if users_df has attributes)
    homophily = {n: 0.0 for n in G.nodes()}
    if users_df is not None and "node" in users_df.columns and "attribute" in users_df.columns:
        attr = users_df.set_index("node")["attribute"].to_dict()
        for n in G.nodes():
            neigh = list(G.neighbors(n))
            if not neigh:
                homophily[n] = 0.0
            else:
                same = sum(1 for v in neigh if attr.get(v) == attr.get(n))
                homophily[n] = same / len(neigh)

    df = pd.DataFrame.from_records(
        [(n, deg.get(n, 0), pr.get(n, 0), eig.get(n, 0), between.get(n, 0), homophily.get(n, 0))
         for n in G.nodes()],
        columns=["node", "degree", "pagerank", "eigenvector", "betweenness", "homophily"]
    ).set_index("node")
    return df


def detect_louvain(G):
    partition = community_louvain.best_partition(G)
    return partition


def structural_homophily_by_community(G, partition, df):
    # compute fraction of neighbors in same detected community
    for n in G.nodes():
        neigh = list(G.neighbors(n))
        if not neigh:
            df.at[n, "homophily"] = 0.0
            continue
        same = sum(1 for v in neigh if partition.get(v) == partition.get(n))
        df.at[n, "homophily"] = same / len(neigh)
    return df


def normalize_and_score(df, weights=None):
    if weights is None:
        weights = {"pagerank": 0.35, "degree": 0.25, "betweenness": 0.15, "eigenvector": 0.10, "homophily": 0.15}
    scaler = MinMaxScaler()
    numeric_cols = ["degree", "pagerank", "eigenvector", "betweenness", "homophily"]
    scaled_vals = scaler.fit_transform(df[numeric_cols].fillna(0.0))
    scaled = pd.DataFrame(scaled_vals, index=df.index, columns=numeric_cols)
    df = df.copy()
    df["influence"] = 0.0
    for k, w in weights.items():
        df["influence"] += scaled[k] * w
    for c in numeric_cols:
        df[c + "_norm"] = scaled[c]
    return df


def sample_nodes_preserve_communities(partition_full, total_budget):
    comm_sizes = defaultdict(int)
    for n, c in partition_full.items():
        comm_sizes[c] += 1
    total_nodes = sum(comm_sizes.values())
    comms = sorted(comm_sizes.keys())
    quotas = {}
    for c in comms:
        prop = comm_sizes[c] / total_nodes
        q = max(MIN_PER_COMMUNITY, int(round(prop * total_budget)))
        quotas[c] = q
    assigned = sum(quotas.values())
    if assigned != total_budget:
        diff = total_budget - assigned
        comm_by_size = sorted(comms, key=lambda x: comm_sizes[x], reverse=True)
        idx = 0
        while diff != 0:
            c = comm_by_size[idx % len(comm_by_size)]
            if diff > 0:
                quotas[c] += 1
                diff -= 1
            else:
                if quotas[c] > MIN_PER_COMMUNITY:
                    quotas[c] -= 1
                    diff += 1
            idx += 1
    return quotas


def select_leader_deputy(df, partition):
    comm_members = defaultdict(list)
    for n, comm in partition.items():
        comm_members[comm].append(n)

    rows = []
    for comm, members in sorted(comm_members.items()):
        members_sorted = sorted(members, key=lambda n: df.at[n, "influence"], reverse=True)
        leader = members_sorted[0] if len(members_sorted) >= 1 else None
        deputy = members_sorted[1] if len(members_sorted) >= 2 else None
        if leader is not None:
            rows.append({"community": comm, "role": "Leader", "node": leader,
                         "influence": float(df.at[leader, "influence"]),
                         "degree": int(df.at[leader, "degree"])})
        if deputy is not None:
            rows.append({"community": comm, "role": "Deputy", "node": deputy,
                         "influence": float(df.at[deputy, "influence"]),
                         "degree": int(df.at[deputy, "degree"])})
    return pd.DataFrame(rows)


def save_communities_csv(partition, out_csv):
    comm_map = defaultdict(list)
    for node, comm in partition.items():
        comm_map[comm].append(node)
    comm_rows = []
    for comm, members in sorted(comm_map.items()):
        comm_rows.append({
            "community": comm,
            "size": len(members),
            "members": ";".join(map(str, sorted(members)))
        })
    pd.DataFrame(comm_rows).to_csv(out_csv, index=False)


def visualize_communities_with_only_leaders(G_full, partition_full, leader_deputy_df, out_png,
                                            community_alpha=0.25,
                                            centroid_scale=1.05,
                                            base_leader_size=420,
                                            base_deputy_size=240,
                                            max_legend_entries=20,
                                            label_leaders=True):
    """
    Visualize community regions (filled circles) and only the Leader/Deputy nodes.
    - G_full: the full graph (used only to compute positions)
    - partition_full: dict node -> community
    - leader_deputy_df: DataFrame with columns ['community','role','node',...]
    - out_png: filename to save
    """
    # 1) compute positions for EVERY node (we won't draw them, only use for centroids)
    pos = nx.spring_layout(G_full, seed=RANDOM_SEED, iterations=200)

    # 2) compute per-community centroid and radius (based on member positions)
    comm_to_positions = {}
    for n, c in partition_full.items():
        # safety: some nodes might be missing in pos if they were isolated; skip them
        if n not in pos:
            continue
        comm_to_positions.setdefault(c, []).append(pos[n])

    comm_centroids = {}
    comm_radius = {}
    for c, pts in comm_to_positions.items():
        xs = [p[0] for p in pts]
        ys = [p[1] for p in pts]
        cx = sum(xs) / len(xs)
        cy = sum(ys) / len(ys)
        comm_centroids[c] = (cx, cy)
        maxd = 0.0
        for x, y in zip(xs, ys):
            d = math.hypot(x - cx, y - cy)
            if d > maxd:
                maxd = d
        comm_radius[c] = maxd * centroid_scale if maxd > 1e-12 else 0.05

    # 3) prepare colors
    comms = sorted(comm_centroids.keys())
    ncomms = len(comms)
    cmap = plt.get_cmap("tab20") if ncomms <= 20 else plt.get_cmap("tab20b")
    color_map = {c: cmap(i % cmap.N) for i, c in enumerate(comms)}

    # 4) build figure and draw community patches (no nodes)
    fig, ax = plt.subplots(figsize=(14, 9))
    for c in comms:
        cx, cy = comm_centroids[c]
        r = comm_radius[c]
        circ = Circle((cx, cy), r, color=color_map[c], alpha=community_alpha, zorder=1, linewidth=0)
        ax.add_patch(circ)

    # 5) plot leaders and deputies using the same positions (from pos)
    if leader_deputy_df is None or leader_deputy_df.empty:
        leaders = []
        deputies = []
    else:
        leaders = leader_deputy_df[leader_deputy_df['role'] == 'Leader']['node'].tolist()
        deputies = leader_deputy_df[leader_deputy_df['role'] == 'Deputy']['node'].tolist()

    # Filter to nodes present in pos (safety)
    leaders = [n for n in leaders if n in pos]
    deputies = [n for n in deputies if n in pos]

    # deputy scatter (diamond)
    if deputies:
        dx = [pos[n][0] for n in deputies]
        dy = [pos[n][1] for n in deputies]
        dcolors = [color_map[partition_full[n]] for n in deputies]
        ax.scatter(dx, dy, s=base_deputy_size, marker='D', edgecolors='k', linewidths=0.9,
                   c=dcolors, zorder=3, label='Deputy')

    # leader scatter (star)
    if leaders:
        lx = [pos[n][0] for n in leaders]
        ly = [pos[n][1] for n in leaders]
        lcolors = [color_map[partition_full[n]] for n in leaders]
        ax.scatter(lx, ly, s=base_leader_size, marker='*', edgecolors='k', linewidths=1.2,
                   c=lcolors, zorder=4, label='Leader')

    # 6) optionally label leaders with their node id (small, above the marker)
    if label_leaders:
        for n in leaders:
            x, y = pos[n]
            ax.text(x, y + 0.01, str(n), fontsize=8, fontweight='bold', ha='center', zorder=5)

    # 7) aesthetics: no axes, tight, legend shows only Leader/Deputy + maybe few communities
    ax.set_axis_off()
    ax.set_title("Communities (colored) — only Leaders (*) and Deputies (♦) shown", fontsize=16)

    # Add a compact legend: Leader and Deputy
    handles, labels = ax.get_legend_handles_labels()
    if handles:
        ax.legend(handles, labels, loc='lower left', framealpha=0.9)

    # If you want a small legend mapping colors -> community id (only when few communities)
    if ncomms <= max_legend_entries:
        from matplotlib.patches import Patch
        patches = [Patch(facecolor=color_map[c], edgecolor='k', label=f"Community {c}", alpha=0.7) for c in comms]
        ax.legend(handles=patches + handles, bbox_to_anchor=(1.02, 0.5), loc='center left', borderaxespad=0., framealpha=0.9)

    plt.tight_layout()
    plt.savefig(out_png, dpi=200, bbox_inches='tight')
    plt.close(fig)
    print(f"Saved leaders-only visualization as: {out_png}")


def main():
    # 1) load full graph (search for edges.csv)
    try:
        G_full, used_path = load_graph()
    except FileNotFoundError as e:
        print(str(e))
        return

    # 2) try to load users (optional)
    try:
        users_df = pd.read_csv(find_file_in_tree(USERS_CSV) or USERS_CSV)
    except Exception:
        users_df = None

    # 3) detect communities on full graph to guide sampling
    partition_full = detect_louvain(G_full)
    n_full = G_full.number_of_nodes()
    print(f"Full graph: {n_full} nodes, {G_full.number_of_edges()} edges. Detected {len(set(partition_full.values()))} communities (on full graph).")

    # 4) sample if graph is large
    if n_full > (SAMPLE_SIZE + 50):  # only sample when definitely larger
        budget = min(SAMPLE_SIZE, n_full)
        quotas = sample_nodes_preserve_communities(partition_full, budget)
        sampled_nodes = []
        # build mapping community -> nodes
        comm_to_nodes = defaultdict(list)
        for n, c in partition_full.items():
            comm_to_nodes[c].append(n)
        for c, q in quotas.items():
            members = comm_to_nodes[c]
            if len(members) <= q:
                chosen = members
            else:
                chosen = random.sample(members, q)
            sampled_nodes.extend(chosen)
        sampled_nodes = sorted(set(sampled_nodes))
        G = G_full.subgraph(sampled_nodes).copy()
        print(f"Sampled subgraph: {G.number_of_nodes()} nodes, {G.number_of_edges()} edges (budget {budget}).")
    else:
        G = G_full
        print("Using full graph (no sampling).")

    # 5) run Louvain on the graph we'll use (sampled or full)
    partition = detect_louvain(G)
    print(f"Using {len(set(partition.values()))} communities for selection/visualization.")

    # 6) compute features and homophily
    features = compute_features(G, users_df)
    features = structural_homophily_by_community(G, partition, features)
    features = normalize_and_score(features)

    # 7) select leader + deputy per community
    leaders_df = select_leader_deputy(features, partition)

    # 8) save outputs
    features.reset_index().to_csv(f"{OUT_PREFIX}_nodes_features.csv", index=False)
    save_communities_csv(partition, f"{OUT_PREFIX}_communities.csv")
    leaders_df.to_csv(f"{OUT_PREFIX}_leaders.csv", index=False)
    print("Saved outputs: per_community_nodes_features.csv, per_community_communities.csv, per_community_leaders.csv")

    # 9) visualization (use full graph centroids for community blobs but show only leaders/deputies)
    visualize_communities_with_only_leaders(G_full, partition_full, leaders_df, f"{OUT_PREFIX}_leaders_only_visualization.png")


if __name__ == "__main__":
    main()
