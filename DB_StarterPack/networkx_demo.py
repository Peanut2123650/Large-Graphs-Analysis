# networkx_demo.py
# Extended example of running graph algorithms from the exported CSV.
# Usage:
#   pip install pandas networkx python-louvain matplotlib
#   python networkx_demo.py

import pandas as pd
import networkx as nx
import ast
import matplotlib.pyplot as plt

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
#SubGraph selection
# ----------------------------
def multi_attribute_subgraph(G, users, **kwargs):

    selected_ids = set(users["_id"])  # start with all users

    for attr, val in kwargs.items():
        if val is not None:
            # Special handling for languages and interests: check for any match in lists
            if attr in ["languages", "interests"]:
                matched = users[users[attr].apply(
                    lambda x: bool(set(val) & set(parse_list_field(x)))
                )]["_id"]
            elif isinstance(val, (list, set)):
                matched = users[users[attr].isin(val)]["_id"]
            else:
                matched = users[users[attr] == val]["_id"]
            selected_ids = selected_ids & set(matched)

    return G.subgraph(selected_ids).copy()

'''
# Users in Mumbai, age 20-25, interested in 'Music', primary language 'en'
sub_g = multi_attribute_subgraph(
    G, users,
    city='Mumbai',
    age=range(20, 26),
    interests=['Music'],
    primaryLang='en'
)
print("Filtered nodes:", sub_g.number_of_nodes(), "Filtered edges:", sub_g.number_of_edges())

'''
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
comm_df = None
if community_louvain:
    partition = community_louvain.best_partition(G, weight="weight")
    communities_set = set(partition.values())
    print(f"\nLouvain communities found: {len(communities_set)}")

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

    
# ----------------------------
# Visualization of communities
# ----------------------------
import matplotlib.pyplot as plt

if 'comm_df' in globals() and comm_df is not None:
    print("\nDrawing community visualization...")

    # Use spring layout with larger k and more iterations for better separation
    pos = nx.spring_layout(G, k=0.5, iterations=200, seed=42)

    # Get community mapping
    comm_map = dict(zip(comm_df["_id"], comm_df["louvain_comm"]))
    pr_map = dict(zip(pr_df["_id"], pr_df["pagerank"]))

    # Assign colors based on community
    communities = list(set(comm_map.values()))
    color_map = {c: plt.cm.tab20(i % 20) for i, c in enumerate(communities)}

    node_colors = [color_map[comm_map[n]] for n in G.nodes()]
    # Scale node sizes by PageRank (rescaled for visibility)
    min_size, max_size = 50, 1000
    pr_values = list(pr_map.values())
    pr_min, pr_max = min(pr_values), max(pr_values)
    node_sizes = [
        min_size + (max_size - min_size) * (pr_map.get(n, pr_min) - pr_min) / (pr_max - pr_min)
        for n in G.nodes()
    ]

    plt.figure(figsize=(14, 12))
    nx.draw_networkx(
        G,
        pos,
        with_labels=False,
        node_color=node_colors,
        node_size=node_sizes,
        edge_color="lightgray",
        alpha=0.7
    )

    # Legend for communities
    for comm_id, color in color_map.items():
        plt.scatter([], [], c=[color], label=f"Community {comm_id}")
    plt.legend(scatterpoints=1, fontsize=10)

    plt.title("Social Network Communities (colored by Louvain, size = PageRank)", fontsize=16)
    plt.axis("off")
    plt.tight_layout()
    plt.savefig("network_communities.png", dpi=300)
    plt.show()

    print("Saved visualization as network_communities.png")
