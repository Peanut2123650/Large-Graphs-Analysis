# networkx_demo.py
# Extended example of running graph algorithms from the exported CSV.
# Usage:
#   pip install pandas networkx python-louvain
#   python networkx_demo.py

import pandas as pd
import networkx as nx
import ast  # for safely parsing list strings

try:
    import community as community_louvain  # python-louvain package
except ImportError:
    community_louvain = None

# ----------------------------
# Load data
# ----------------------------
edges = pd.read_csv("edges.csv")
users = pd.read_csv("users.csv")

# Ensure IDs are strings
edges["src"] = edges["src"].astype(str)
edges["dst"] = edges["dst"].astype(str)
users["_id"] = users["_id"].astype(str)

# ----------------------------
# Build undirected friend graph
# ----------------------------
friend_edges = edges[edges["type"] == "friend"][["src", "dst", "weight"]]
G = nx.Graph()
for _, row in friend_edges.iterrows():
    G.add_edge(row["src"], row["dst"], weight=float(row.get("weight", 1.0)))

print("Nodes:", G.number_of_nodes(), "Edges:", G.number_of_edges())

# ----------------------------
# PageRank
# ----------------------------
pr = nx.pagerank(G, weight="weight")
pr_top = sorted(pr.items(), key=lambda x: x[1], reverse=True)[:10]

print("\nTop 10 PageRank nodes:")
for node, score in pr_top:
    name_row = users.loc[users["_id"] == node, "name"]
    name = name_row.values[0] if not name_row.empty else "Unknown"
    print(f"{node} ({name}) -> {score:.5f}")

# Save PageRank with names
pr_df = pd.DataFrame(
    [(uid, users.loc[users["_id"] == uid, "name"].values[0] if not users.loc[users["_id"] == uid].empty else "Unknown", score)
     for uid, score in pr.items()],
    columns=["_id", "name", "pagerank"]
)
pr_df.to_csv("pagerank.csv", index=False)
print("Wrote pagerank.csv")

# ----------------------------
# Community detection
# ----------------------------
if community_louvain:
    partition = community_louvain.best_partition(G, weight="weight")
    communities = set(partition.values())
    print(f"\nLouvain communities found: {len(communities)}")

    comm_df = pd.DataFrame(
        [(uid, users.loc[users["_id"] == uid, "name"].values[0] if not users.loc[users["_id"] == uid].empty else "Unknown", comm)
         for uid, comm in partition.items()],
        columns=["_id", "name", "louvain_comm"]
    )
    comm_df.to_csv("communities.csv", index=False)
    print("Wrote communities.csv")
else:
    communities_list = list(nx.algorithms.community.label_propagation_communities(G))
    print(f"\nLabel Propagation communities found: {len(communities_list)}")

# ----------------------------
# Friend recommendation
# ----------------------------
def parse_list_field(field_value):
    """Safely parse list-like string fields from CSV."""
    try:
        if pd.isna(field_value) or field_value == "":
            return []
        if isinstance(field_value, list):
            return field_value
        return ast.literal_eval(field_value)
    except:
        return []

def recommend_friends(user_id, top_k=5):
    """Recommend friends for a given user_id based on attributes + common neighbors."""
    if user_id not in G:
        return []

    current_friends = set(G.neighbors(user_id))
    candidates = set(G.nodes()) - current_friends - {user_id}

    user_row = users[users["_id"] == user_id]
    if user_row.empty:
        return []

    user_langs = set([lang['code'] for lang in parse_list_field(user_row["languages"].values[0])])
    user_city = str(user_row["city"].values[0])
    user_interests = set(parse_list_field(user_row["interests"].values[0]))

    scored = []
    for cand in candidates:
        cand_row = users[users["_id"] == cand]
        if cand_row.empty:
            continue
        score = 0

        # +1 if same city
        if str(cand_row["city"].values[0]) == user_city:
            score += 1

        # + languages overlap
        cand_langs = set([lang['code'] for lang in parse_list_field(cand_row["languages"].values[0])])
        score += len(user_langs & cand_langs)

        # + interests overlap
        cand_interests = set(parse_list_field(cand_row["interests"].values[0]))
        score += len(user_interests & cand_interests)

        # + common neighbors count
        common_neighbors = len(set(nx.common_neighbors(G, user_id, cand)))
        score += common_neighbors

        if score > 0:
            scored.append((cand, score))

    scored.sort(key=lambda x: x[1], reverse=True)
    return scored[:top_k]

# Example: recommend for top PageRank user
example_user = pr_top[0][0]
print(f"\nFriend recommendations for {example_user}:")
for cand, score in recommend_friends(example_user, top_k=5):
    name_row = users.loc[users["_id"] == cand, "name"]
    name = name_row.values[0] if not name_row.empty else "Unknown"
    print(f"  {cand} ({name}) score={score}")
