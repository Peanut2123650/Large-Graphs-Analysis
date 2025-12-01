# DB_Starter_Pack/influence_by_formula.py
# Same behaviour as original but optimized for larger graphs (approx betweenness, faster k-hop & homophily)

import os
import pandas as pd
import networkx as nx
import numpy as np
from sklearn.preprocessing import MinMaxScaler
from collections import deque
import random
import time
from tqdm import tqdm

# --------------------------
# CONFIG FLAGS
# --------------------------
USE_GENERATED_ACTIVENESS = True   # True = use random weekly activeness
RANDOM_SEED = 42                  # for reproducibility
COMPUTE_BETWEENNESS = True        # set False to skip betweenness
BETWEENNESS_SAMPLE_K = None       # if None and graph large, script picks a sensible sample
K_HOP = 3                         # for k-hop coverage

random.seed(RANDOM_SEED)
np.random.seed(RANDOM_SEED)

# --------------------------
# PATH SETUP
# --------------------------
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_DIR = os.path.join(BASE_DIR, "data")

# keep same defaults as your original script (you used top_200_edges.csv previously)
edges_path = os.path.join(DATA_DIR, "top_200_edges.csv")
output_path = os.path.join(DATA_DIR, "influence_scores_formula_200.csv")

# --------------------------
# LOAD EDGES
# --------------------------
if not os.path.exists(edges_path):
    raise FileNotFoundError(f"Edges file not found: {edges_path}")

edges_df = pd.read_csv(edges_path)

# --------------------------
# BUILD GRAPH (directed with friend -> two-way)
# --------------------------
G = nx.DiGraph()
for _, row in edges_df.iterrows():
    src, dst = str(row["src"]), str(row["dst"])
    e_type = row.get("type", "friend")
    try:
        w = float(row.get("weight", 1.0))
    except Exception:
        w = 1.0

    if e_type == "friend":
        G.add_edge(src, dst, weight=w)
        G.add_edge(dst, src, weight=w)
    else:
        G.add_edge(src, dst, weight=w)

print(f"Graph loaded: {G.number_of_nodes()} nodes, {G.number_of_edges()} edges")

nodes_list = list(G.nodes())
n_nodes = len(nodes_list)

# --------------------------
# ACTIVENESS GENERATOR (unchanged)
# --------------------------
def generate_weekly_activeness(nodes):
    act_map = {}
    for n in nodes:
        r = random.random()
        if r < 0.60:
            base = random.uniform(0.10, 0.50)
        elif r < 0.90:
            base = random.uniform(0.50, 0.80)
        else:
            base = random.uniform(0.80, 1.00)
        weekly = base + random.uniform(-0.05, 0.05)
        weekly = max(0, min(1, weekly))
        act_map[n] = weekly
    return pd.DataFrame({"node": list(act_map.keys()), "A": list(act_map.values())})

# --------------------------
# CENTRALITY MEASURES
# --------------------------
print("\nComputing centralities...")

# 1) Betweenness (approximate if graph large)
bet = {}
if COMPUTE_BETWEENNESS:
    print("Computing betweenness...")
    start = time.time()
    # if few nodes, do exact; otherwise approximate using sampling
    if n_nodes <= 5000:
        try:
            bet = nx.betweenness_centrality(G, weight="weight", normalized=True)
        except Exception as e:
            print("Exact betweenness failed, falling back to approximation:", e)
            bet = {n: 0.0 for n in nodes_list}
    else:
        # choose k sample size
        k_sample = BETWEENNESS_SAMPLE_K
        if k_sample is None:
            k_sample = min(250, max(50, n_nodes // 60))  # heuristic: scale with graph size
        print(f"Graph large ({n_nodes} nodes). Using approximate betweenness with sample k={k_sample}")
        import random as _rnd
        _rnd.seed(RANDOM_SEED)
        sample_sources = _rnd.sample(nodes_list, min(k_sample, n_nodes))
        try:
            # use betweenness_centrality_subset for sampled sources -> cheaper
            bet = nx.betweenness_centrality_subset(G, sources=sample_sources, targets=nodes_list, normalized=True, weight='weight')
        except Exception as e:
            print("Approx betweenness subset failed:", e)
            # fallback: compute zero or very approximate degree-based proxy
            bet = {n: 0.0 for n in nodes_list}
    print(f" Betweenness done in {time.time()-start:.1f}s")
else:
    bet = {n: 0.0 for n in nodes_list}

# Ensure bet has entries for all nodes
for n in nodes_list:
    if n not in bet:
        bet[n] = 0.0

# 2) Eigenvector (per weakly-connected component)
eigen = {}
print("Computing eigenvector centrality per weak component...")
for comp in nx.weakly_connected_components(G):
    sub_nodes = list(comp)
    sub = G.subgraph(sub_nodes)
    try:
        ev = nx.eigenvector_centrality_numpy(sub, weight="weight")
        eigen.update(ev)
    except Exception:
        try:
            ev = nx.eigenvector_centrality(sub, max_iter=500, tol=1e-6, weight="weight")
            eigen.update(ev)
        except Exception:
            # on failure, set zero for nodes in this component
            for n in sub_nodes:
                eigen[n] = 0.0

# fill missing nodes
for n in nodes_list:
    if n not in eigen:
        eigen[n] = 0.0

# 3) Closeness (undirected)
print("Computing closeness centrality (undirected)...")
try:
    closeness = nx.closeness_centrality(G.to_undirected())
except Exception:
    print("Closeness failed; setting zeros")
    closeness = {n: 0.0 for n in nodes_list}

# 4) Popularity P (degree)
deg = dict(G.degree())

# --------------------------
# 5) K-hop coverage (optimized)
# --------------------------
print(f"Computing {K_HOP}-hop reach (optimized)...")
# adjacency dict (undirected neighbors)
G_und = G.to_undirected()
adj = {n: set(G_und.neighbors(n)) for n in nodes_list}

def khop_coverage_fraction_fast(source, k=K_HOP):
    seen = {source}
    frontier = {source}
    for _ in range(k):
        # union of neighbors of frontier
        nxt = set()
        for node in frontier:
            nxt |= adj.get(node, set())
        nxt -= seen
        if not nxt:
            break
        seen |= nxt
        frontier = nxt
    return (len(seen) - 1) / (n_nodes - 1) if n_nodes > 1 else 0.0

K = {}
# iterate with progress bar
for i, node in enumerate(tqdm(nodes_list, desc=f"{K_HOP}-hop")):
    K[node] = khop_coverage_fraction_fast(node, k=K_HOP)

# --------------------------
# 6) Homophily H (avg Jaccard over neighbors - optimized)
# --------------------------
print("Computing homophily (avg neighbor Jaccard on undirected neighbors)...")
# Precompute neighbor sets (already have adj)
H = {}
for u in tqdm(nodes_list, desc="homophily"):
    nbrs = adj.get(u, set())
    if not nbrs:
        H[u] = 0.0
        continue
    sim_sum = 0.0
    cnt = 0
    for v in nbrs:
        Nv = adj.get(v, set())
        union = nbrs | Nv
        inter = nbrs & Nv
        if len(union) == 0:
            j = 0.0
        else:
            j = len(inter) / len(union)
        sim_sum += j
        cnt += 1
    H[u] = (sim_sum / cnt) if cnt > 0 else 0.0

# --------------------------
# BUILD DATAFRAME (same columns as original)
# --------------------------
print("Assembling dataframe of metrics...")
df = pd.DataFrame({
    "node": nodes_list,
    "E": [eigen.get(n, 0.0) for n in nodes_list],
    "B": [bet.get(n, 0.0) for n in nodes_list],
    "A": [closeness.get(n, 0.0) for n in nodes_list],   # placeholder activeness (may override)
    "P": [deg.get(n, 0.0) for n in nodes_list],
    "K": [K.get(n, 0.0) for n in nodes_list],
    "H": [H.get(n, 0.0) for n in nodes_list],
})

# --------------------------
# OPTIONAL: override A with generated activeness
# --------------------------
if USE_GENERATED_ACTIVENESS:
    print("Using generated weekly activeness (A)...")
    act_df = generate_weekly_activeness(nodes_list)
    df = df.drop(columns=["A"])
    df = df.merge(act_df.rename(columns={"node":"node", "A":"A"}), on="node", how="left")

# --------------------------
# CORRELATION (same)
# --------------------------
metrics = ["E", "B", "A", "P", "K", "H"]
print("\n📌 CORRELATION MATRIX:")
print(df[metrics].corr())

# --------------------------
# NORMALIZATION
# --------------------------
scaler = MinMaxScaler()
df[[m + "_n" for m in metrics]] = scaler.fit_transform(df[metrics])

# --------------------------
# FORMULA (unchanged)
# --------------------------
df["Base"] = (
    0.6*df["E_n"] +
    0.3*df["B_n"] +
    0.05*df["H_n"] +
    0.05*df["A_n"]
)

df["Reach"] = 0.6*df["P_n"] + 0.4*df["K_n"]

df["Influence_I"] = 0.7*df["Base"] + 0.3*df["Reach"]

# --------------------------
# SAVE OUTPUT
# --------------------------
df.sort_values("Influence_I", ascending=False, inplace=True)
df.reset_index(drop=True, inplace=True)

df.to_csv(output_path, index=False)
print("\nSaved influence scores to:", output_path)

print("\n🏆 Top 10 Influential Nodes:")
print(df[["node","Influence_I","Base","Reach"]].head(10))
