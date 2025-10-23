# DB_Starter_Pack/influence_analysis.py
"""
Influence Analysis Module
-------------------------
Computes Betweenness + Eigenvector Centrality,
normalizes them, and assigns an overall influence weight.
Handles disconnected graphs safely.

Purpose:
Identify influential users in the social graph — nodes that act
as bridges (betweenness) or are highly connected within their communities
(eigenvector). Used for “Suggested Profiles” or “Recommended Connections”
when nodes are not directly linked but are likely to form groups.
"""

import os
import pandas as pd
import networkx as nx
from sklearn.preprocessing import MinMaxScaler

# ----------------------------
# Path Setup
# ----------------------------
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_DIR = os.path.join(BASE_DIR, "data")

edges_path = os.path.join(DATA_DIR, "edges.csv")
output_path = os.path.join(DATA_DIR, "centrality_influence_scores.csv")

# ----------------------------
# Load Graph Data
# ----------------------------
edges_df = pd.read_csv(edges_path)

# Create directed graph (for follow) + mutual edges (for friend)
G = nx.DiGraph()
for _, row in edges_df.iterrows():
    src, dst = str(row["src"]), str(row["dst"])
    edge_type = row.get("type", "friend")
    weight = float(row.get("weight", 1.0))

    if edge_type == "friend":
        # bidirectional friendship
        G.add_edge(src, dst, weight=weight)
        G.add_edge(dst, src, weight=weight)
    else:
        G.add_edge(src, dst, weight=weight)

print(f"✅ Graph loaded: {G.number_of_nodes()} nodes, {G.number_of_edges()} edges")

# ----------------------------
# Compute Centralities
# ----------------------------
print("⏳ Computing Betweenness Centrality...")
betweenness = nx.betweenness_centrality(G, weight="weight", normalized=True)

print("⏳ Computing Eigenvector Centrality (component-safe)...")
eigenvector = {}

# compute eigenvector centrality per connected component
for component in nx.weakly_connected_components(G):
    subgraph = G.subgraph(component)
    try:
        ev_sub = nx.eigenvector_centrality(subgraph, weight="weight", max_iter=500)
        eigenvector.update(ev_sub)
    except nx.PowerIterationFailedConvergence:
        # assign 0 to nodes in components that failed to converge
        for n in subgraph.nodes():
            eigenvector[n] = 0.0

# ----------------------------
# Combine and Normalize
# ----------------------------
df = pd.DataFrame({
    "node": list(G.nodes()),
    "betweenness": [betweenness.get(n, 0.0) for n in G.nodes()],
    "eigenvector": [eigenvector.get(n, 0.0) for n in G.nodes()]
})

# Normalize both metrics to [0,1]
scaler = MinMaxScaler()
df[["betweenness_norm", "eigenvector_norm"]] = scaler.fit_transform(df[["betweenness", "eigenvector"]])

# ----------------------------
# Compute Influence Weight
# ----------------------------
# Equal weighting: can adjust alpha if you want to prioritize one measure
alpha = 0.5
df["influence_weight"] = alpha * df["betweenness_norm"] + (1 - alpha) * df["eigenvector_norm"]

# Rank and sort
df.sort_values("influence_weight", ascending=False, inplace=True)
df.reset_index(drop=True, inplace=True)

# Save results
df.to_csv(output_path, index=False)
print(f"✅ Influence scores saved to: {output_path}")

# Display top 10
print("\n🏆 Top 10 Influential Nodes:")
print(df.head(10))
