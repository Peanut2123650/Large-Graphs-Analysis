# networkx_demo.py
# Updated minimally for 15000-node CSV compatibility — NO feature changes

import pandas as pd
import networkx as nx
import ast
import matplotlib.pyplot as plt

try:
    import community as community_louvain
except ImportError:
    community_louvain = None

# ----------------------------
# Utility: Parse field
# ----------------------------
def parse_list_field(field_value):
    """Safely parse list-like string fields from CSV."""
    try:
        if pd.isna(field_value) or field_value == "":
            return []

        # If already list
        if isinstance(field_value, list):
            return field_value

        # If comma-separated string: turn into list
        if isinstance(field_value, str) and "," in field_value:
            return [x.strip() for x in field_value.split(",") if x.strip()]

        # Otherwise try literal_eval (for formats like "['a','b']")
        return ast.literal_eval(field_value)

    except:
        return []

# ----------------------------
# Load CSV data
# ----------------------------
edges = pd.read_csv("../data/edges.csv")
users = pd.read_csv("../data/users.csv")

# IMPORTANT FIX: Ensure IDs are strings
edges["src"] = edges["src"].astype(str)
edges["dst"] = edges["dst"].astype(str)
users["_id"] = users["_id"].astype(str)

# ----------------------------
# Build undirected friend graph
# ----------------------------
friend_edges = edges[edges["type"] == "friend"][["src", "dst", "weight"]]

G = nx.Graph()
for _, row in friend_edges.iterrows():
    try:
        w = float(row.get("weight", 1.0))
    except:
        w = 1.0
    G.add_edge(row["src"], row["dst"], weight=w)

print("Nodes:", G.number_of_nodes(), "Edges:", G.number_of_edges())

# ----------------------------
# SubGraph selection
# ----------------------------
def multi_attribute_subgraph(G, users, **kwargs):
    selected_ids = set(users["_id"])
    for attr, val in kwargs.items():
        if val is not None:
            if attr in ["languages", "interests"]:
                matched = users[users[attr].apply(
                    lambda x: bool(set(val) & set(parse_list_field(x)))
                )]["_id"]
            elif isinstance(val, (list, set, range)):
                matched = users[users[attr].isin(val)]["_id"]
            else:
                matched = users[users[attr] == val]["_id"]
            selected_ids = selected_ids & set(matched)
    return G.subgraph(selected_ids).copy(), users[users["_id"].isin(selected_ids)]

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

# Save PageRank
pr_df = pd.DataFrame(
    [(uid,
      users.loc[users["_id"] == uid, "name"].values[0] if not users.loc[users["_id"] == uid].empty else "Unknown",
      score)
     for uid, score in pr.items()],
    columns=["_id", "name", "pagerank"]
)
pr_df.to_csv(r"C:\Users\p2123\Desktop\COLLEGE\PROJECT_3rdYear\Social_Network_Project\data\pagerank.csv", index=False)
print("Wrote pagerank.csv")

# ----------------------------
# Community detection
# ----------------------------
comm_df = None
if community_louvain:
    partition = community_louvain.best_partition(G, weight="weight")
    communities_set = set(partition.values())
    print(f"\nLouvain communities found: {len(communities_set)}")

    comm_df = pd.DataFrame(
        [(uid,
          users.loc[users["_id"] == uid, "name"].values[0] if not users.loc[users["_id"] == uid].empty else "Unknown",
          comm)
         for uid, comm in partition.items()],
        columns=["_id", "name", "louvain_comm"]
    )
    comm_df.to_csv(r"C:\Users\p2123\Desktop\COLLEGE\PROJECT_3rdYear\Social_Network_Project\data\communities.csv", index=False)
    print("Wrote communities.csv")
else:
    communities_list = list(nx.algorithms.community.label_propagation_communities(G))
    print(f"\nLabel Propagation communities found: {len(communities_list)}")

# ----------------------------
# Friend recommendation
# ----------------------------
def recommend_friends(user_id, top_k=5):
    if user_id not in G:
        return []

    current_friends = set(G.neighbors(user_id))
    candidates = set(G.nodes()) - current_friends - {user_id}

    user_row = users[users["_id"] == user_id]
    if user_row.empty:
        return []

    user_langs = set(parse_list_field(user_row["languages"].values[0]))
    user_city = str(user_row["city"].values[0])
    user_interests = set(parse_list_field(user_row["interests"].values[0]))

    scored = []
    for cand in candidates:
        cand_row = users[users["_id"] == cand]
        if cand_row.empty:
            continue
        score = 0
        if str(cand_row["city"].values[0]) == user_city:
            score += 1

        cand_langs = set(parse_list_field(cand_row["languages"].values[0]))
        score += len(user_langs & cand_langs)

        cand_interests = set(parse_list_field(cand_row["interests"].values[0]))
        score += len(user_interests & cand_interests)

        common_neighbors = len(set(nx.common_neighbors(G, user_id, cand)))
        score += common_neighbors

        if score > 0:
            scored.append((cand, score))

    scored.sort(key=lambda x: x[1], reverse=True)
    return scored[:top_k]

example_user = pr_top[0][0]
print(f"\nFriend recommendations for {example_user}:")
for cand, score in recommend_friends(example_user, top_k=5):
    name_row = users.loc[users["_id"] == cand, "name"]
    name = name_row.values[0] if not name_row.empty else "Unknown"
    print(f"  {cand} ({name}) score={score}")

# ----------------------------
# Visualization (Optimized for performance)
# ----------------------------
if comm_df is not None:
    print("\nPreparing subgraph for visualization (fast)...")

    # Use top 300 PageRank nodes + neighbors (cap ~800 nodes)
    top_nodes = pr_df.sort_values("pagerank", ascending=False)["_id"].head(300).tolist()
    sub_nodes = set(top_nodes)

    for n in top_nodes:
        for nbr in G.neighbors(n):
            if len(sub_nodes) >= 800:
                break
            sub_nodes.add(nbr)
        if len(sub_nodes) >= 800:
            break

    SG = G.subgraph(sub_nodes).copy()

    print("Subgraph created → nodes:", SG.number_of_nodes(), "edges:", SG.number_of_edges())
    print("Drawing visualization...")

    # Fast spring layout on the small graph
    pos = nx.spring_layout(SG, k=0.5, iterations=60, seed=42)

    # Build color and size maps
    comm_map = dict(zip(comm_df["_id"], comm_df["louvain_comm"]))
    pr_map = dict(zip(pr_df["_id"], pr_df["pagerank"]))
    communities = list(set(comm_map.values()))
    color_map = {c: plt.cm.tab20(i % 20) for i, c in enumerate(communities)}

    node_colors = [color_map.get(comm_map.get(n, 0), "gray") for n in SG.nodes()]

    pr_values = [pr_map.get(n, 0) for n in SG.nodes()]
    pr_min, pr_max = (min(pr_values), max(pr_values)) if pr_values else (0, 1)

    min_size, max_size = 50, 800
    node_sizes = [
        min_size + (max_size - min_size) * (pr_map.get(n, pr_min) - pr_min) / (pr_max - pr_min + 1e-9)
        for n in SG.nodes()
    ]

    plt.figure(figsize=(14, 12))
    nx.draw_networkx(
        SG, pos,
        with_labels=False,
        node_color=node_colors,
        node_size=node_sizes,
        edge_color="lightgray",
        alpha=0.75
    )

    # Legend
    for comm_id, color in color_map.items():
        plt.scatter([], [], c=[color], label=f"Comm {comm_id}")
    plt.legend(scatterpoints=1, fontsize=9)

    plt.title("Community Visualization (Subgraph of Influential Nodes)", fontsize=16)
    plt.axis("off")
    plt.tight_layout()
    plt.savefig(r"C:\Users\p2123\Desktop\COLLEGE\PROJECT_3rdYear\Social_Network_Project\DB_StarterPack\images\network_communities.png", dpi=300)
    try:
        plt.show()
    except Exception:
        pass
    print("Saved visualization as network_communities.png")
