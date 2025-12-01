### !! DOESNOT COMPUTE EIGENVECTOR CENTRALITY

import pandas as pd
import networkx as nx
import random

# ===============================
# 1. Load Graph
# ===============================
edges_df = pd.read_csv(r"C:\Users\p2123\Desktop\COLLEGE\PROJECT_3rdYear\Social_Network_Project\data\edges.csv")

# Build undirected weighted graph
G = nx.Graph()
for _, row in edges_df.iterrows():
    G.add_edge(row["src"], row["dst"], weight=row.get("weight", 1.0))

print(f"Graph loaded: {G.number_of_nodes()} nodes, {G.number_of_edges()} edges")

# ===============================
# 2. Compute Centralities
# ===============================
print("Calculating centrality measures...")

# Degree centrality
degree_centrality = nx.degree_centrality(G)

# Approximate betweenness centrality (faster)
sample_size = min(300, len(G))
betweenness_centrality = nx.betweenness_centrality(G, k=sample_size, normalized=True, seed=42)

# Combine both (equal weighting)
combined_centrality = {
    n: (degree_centrality.get(n, 0) + betweenness_centrality.get(n, 0)) / 2
    for n in G.nodes()
}

# ===============================
# 3. Compute Structural Homophily
# ===============================
print("Calculating structural homophily...")

homophily = {}
for node in G.nodes():
    neighbors = list(G.neighbors(node))
    if not neighbors:
        homophily[node] = 0
        continue

    node_neighbors = set(neighbors)
    sims = []
    for nbr in neighbors:
        nbr_neighbors = set(G.neighbors(nbr))
        intersection = len(node_neighbors & nbr_neighbors)
        union = len(node_neighbors | nbr_neighbors)
        sim = intersection / union if union != 0 else 0
        sims.append(sim)
    homophily[node] = sum(sims) / len(sims)

# ===============================
# 4. Combine Influence Score
# ===============================
alpha = 0.6  # weight for centrality
influence_score = {
    n: alpha * combined_centrality[n] + (1 - alpha) * homophily[n]
    for n in G.nodes()
}

# ===============================
# 5. Save Top 200 Influential Nodes
# ===============================
top_k = 200
top_influencers = sorted(influence_score.items(), key=lambda x: x[1], reverse=True)[:top_k]

result_df = pd.DataFrame(top_influencers, columns=["Node", "Influence_Score"])
print(f"\nTop {top_k} Influential Nodes (based on homophily + centrality):")
print(result_df.head(15))  # show top 15 only for console

# Save to CSV
output_path = r"C:\Users\p2123\Desktop\COLLEGE\PROJECT_3rdYear\Social_Network_Project\data\top_200_influential_nodes.csv"
result_df.to_csv(output_path, index=False)

print(f"\n✅ Saved top {top_k} influential nodes to:\n{output_path}")
