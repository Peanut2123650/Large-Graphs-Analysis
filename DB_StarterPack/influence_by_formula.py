# DB_Starter_Pack/influence_by_formula.py

import os
import pandas as pd
import networkx as nx
import numpy as np
from sklearn.preprocessing import MinMaxScaler
from collections import deque
import random
import time

# --------------------------
# CONFIG FLAGS
# --------------------------
USE_GENERATED_ACTIVENESS = True   # True = use random weekly activeness
RANDOM_SEED = 42                  # for reproducibility
COMPUTE_BETWEENNESS = True        # set False to skip betweenness
BETWEENNESS_SAMPLE_K = None       # e.g., 500 for faster approx, or None
K_HOP = 3                         # for k-hop coverage

random.seed(RANDOM_SEED)
np.random.seed(RANDOM_SEED)

# --------------------------
# PATH SETUP
# --------------------------
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_DIR = os.path.join(BASE_DIR, "data")

edges_path = os.path.join(DATA_DIR, "top_200_edges.csv")
output_path = os.path.join(DATA_DIR, "influence_scores_formula_200.csv")

# Load edges
edges_df = pd.read_csv(edges_path)

# --------------------------
# BUILD GRAPH
# --------------------------
G = nx.DiGraph()
for _, row in edges_df.iterrows():
    src, dst = str(row["src"]), str(row["dst"])
    e_type = row.get("type", "friend")
    w = float(row.get("weight", 1.0))

    if e_type == "friend":
        G.add_edge(src, dst, weight=w)
        G.add_edge(dst, src, weight=w)
    else:
        G.add_edge(src, dst, weight=w)

print(f"Graph loaded: {G.number_of_nodes()} nodes, {G.number_of_edges()} edges")

nodes_list = list(G.nodes())

# --------------------------
# ACTIVENESS GENERATOR
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

# 1) Betweenness
if COMPUTE_BETWEENNESS:
    print("Computing betweenness...")
    start = time.time()
    if BETWEENNESS_SAMPLE_K is None:
        bet = nx.betweenness_centrality(G, weight="weight", normalized=True)
    else:
        bet = nx.betweenness_centrality(G, k=BETWEENNESS_SAMPLE_K, weight="weight",
                                        normalized=True, seed=RANDOM_SEED)
    print(f" Betweenness done in {time.time()-start:.1f}s")
else:
    bet = {n: 0.0 for n in nodes_list}

# 2) Eigenvector (per component)
eigen = {}
print("Computing eigenvector centrality...")
for comp in nx.weakly_connected_components(G):
    sub = G.subgraph(comp)
    try:
        ev = nx.eigenvector_centrality_numpy(sub, weight="weight")
        eigen.update(ev)
    except:
        try:
            ev = nx.eigenvector_centrality(sub, weight="weight", max_iter=500)
            eigen.update(ev)
        except:
            for n in sub.nodes():
                eigen[n] = 0.0

# 3) Closeness (undirected)
closeness = nx.closeness_centrality(G.to_undirected())

# 4) Popularity P (degree)
deg = dict(G.degree())

# 5) K-hop coverage
def khop_coverage_fraction(G, source, k=3):
    visited = {source}
    q = deque([(source, 0)])

    while q:
        node, depth = q.popleft()
        if depth >= k:
            continue

        neighbors = set(G.predecessors(node)) | set(G.successors(node))
        for nb in neighbors:
            if nb not in visited:
                visited.add(nb)
                q.append((nb, depth+1))

    if G.number_of_nodes() <= 1:
        return 0
    return (len(visited) - 1) / (G.number_of_nodes() - 1)

print(f"Computing {K_HOP}-hop reach...")
K = {}
for i, n in enumerate(nodes_list):
    K[n] = khop_coverage_fraction(G, n, k=K_HOP)
    if i % 800 == 0 and i > 0:
        print(f"  processed {i}/{len(nodes_list)}")

# 6) Homophily H (avg Jaccard)
print("Computing homophily...")
G_und = G.to_undirected()

def jaccard(u, v):
    Nu = set(G_und.neighbors(u))
    Nv = set(G_und.neighbors(v))
    if not Nu and not Nv:
        return 0.0
    inter = len(Nu & Nv)
    union = len(Nu | Nv)
    return inter / union if union else 0.0

H = {}
for n in nodes_list:
    nbrs = list(G_und.neighbors(n))
    if not nbrs:
        H[n] = 0.0
    else:
        H[n] = sum(jaccard(n, nb) for nb in nbrs) / len(nbrs)

# --------------------------
# BUILD DATAFRAME
# --------------------------
df = pd.DataFrame({
    "node": nodes_list,
    "E": [eigen[n] for n in nodes_list],
    "B": [bet[n] for n in nodes_list],
    "A": [closeness[n] for n in nodes_list],   # temporary A
    "P": [deg[n] for n in nodes_list],
    "K": [K[n] for n in nodes_list],
    "H": [H[n] for n in nodes_list],
})

# --------------------------
# OPTIONAL: override A with generated activeness
# --------------------------
if USE_GENERATED_ACTIVENESS:
    print("Using generated weekly activeness (A)...")
    act_df = generate_weekly_activeness(nodes_list)
    df = df.drop(columns=["A"])
    df = df.merge(act_df, on="node", how="left")

# --------------------------
# CORRELATION
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
# FORMULA
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
