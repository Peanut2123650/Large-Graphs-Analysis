import pandas as pd
import networkx as nx
import ast

# ----------------------------
# Utility: Parse field
# ----------------------------
def parse_list_field(field_value):
    try:
        if pd.isna(field_value) or field_value == "":
            return []
        if isinstance(field_value, list):
            return field_value
        return ast.literal_eval(field_value)
    except:
        return []

# ----------------------------
# Subgraph Selection
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
            selected_ids &= set(matched)
    return G.subgraph(selected_ids).copy(), users[users["_id"].isin(selected_ids)]

# ----------------------------
# PageRank
# ----------------------------
def pagerank_with_names(G, users, top_k=10, csv_out=None):
    pr = nx.pagerank(G, weight="weight")
    pr_top = sorted(pr.items(), key=lambda x: x[1], reverse=True)[:top_k]
    print("\nTop PageRank nodes:")
    for node, score in pr_top:
        name_row = users.loc[users["_id"] == node, "name"]
        name = name_row.iloc[0] if not name_row.empty else "Unknown"
        print(f"{node} ({name}) -> {score:.5f}")
    if csv_out:
        pr_df = pd.DataFrame(
            [(uid,
              users.loc[users["_id"] == uid, "name"].iloc[0] if not users.loc[users['_id'] == uid].empty else "Unknown",
              score)
             for uid, score in pr.items()],
            columns=["_id", "name", "pagerank"]
        )
        pr_df.to_csv(csv_out, index=False)
        print(f"Wrote {csv_out}")
    return pr, pr_top

# ----------------------------
# Community Detection
# ----------------------------
def detect_communities(G, users, community_louvain_module=None, csv_out=None):
    if community_louvain_module:
        partition = community_louvain_module.best_partition(G, weight="weight")
        communities = set(partition.values())
        print(f"\nLouvain communities found: {len(communities)}")
        if csv_out:
            comm_df = pd.DataFrame(
                [(uid,
                  users.loc[users["_id"] == uid, "name"].iloc[0] if not users.loc[users["_id"] == uid].empty else "Unknown",
                  comm)
                 for uid, comm in partition.items()],
                columns=["_id", "name", "louvain_comm"]
            )
            comm_df.to_csv(csv_out, index=False)
            print(f"Wrote {csv_out}")
        return partition
    else:
        communities_list = list(nx.algorithms.community.label_propagation_communities(G))
        print(f"\nLabel Propagation communities found: {len(communities_list)}")
        return communities_list

# ----------------------------
# Friend Recommendation
# ----------------------------
def recommend_friends(G, users, user_id, top_k=5):
    if user_id not in G:
        return []
    current_friends = set(G.neighbors(user_id))
    candidates = set(G.nodes()) - current_friends - {user_id}
    user_row = users[users["_id"] == user_id]
    if user_row.empty:
        return []
    user_row = user_row.iloc[0]
    user_langs = set(lang['code'] for lang in parse_list_field(user_row.get("languages", [])))
    user_city = str(user_row.get("city", ""))
    user_interests = set(parse_list_field(user_row.get("interests", [])))
    scored = []
    for cand in candidates:
        cand_row = users[users["_id"] == cand]
        if cand_row.empty:
            continue
        cand_row = cand_row.iloc[0]
        score = 0
        if str(cand_row.get("city", "")) == user_city:
            score += 1
        cand_langs = set(lang['code'] for lang in parse_list_field(cand_row.get("languages", [])))
        score += len(user_langs & cand_langs)
        cand_interests = set(parse_list_field(cand_row.get("interests", [])))
        score += len(user_interests & cand_interests)
        common_neighbors = len(list(nx.common_neighbors(G, user_id, cand)))
        score += common_neighbors
        if score > 0:
            scored.append((cand, score))
    scored.sort(key=lambda x: x[1], reverse=True)
    return scored[:top_k]

# ----------------------------
# Main Demo
# ----------------------------
if __name__ == '__main__':
    edges = pd.read_csv("data/edges.csv")
    users = pd.read_csv("data/users.csv")
    edges["src"] = edges["src"].astype(str)
    edges["dst"] = edges["dst"].astype(str)
    users["_id"] = users["_id"].astype(str)

    friend_edges = edges[edges["type"] == "friend"][["src", "dst", "weight"]]
    G = nx.Graph()
    for _, row in friend_edges.iterrows():
        G.add_edge(row["src"], row["dst"], weight=float(row.get("weight", 1.0)))
    print("Nodes:", G.number_of_nodes(), "Edges:", G.number_of_edges())

    # PageRank
    pr, pr_top = pagerank_with_names(G, users, top_k=10, csv_out="data/pagerank.csv")

    # Communities
    try:
        import community as community_louvain
    except ImportError:
        community_louvain = None
    detect_communities(G, users, community_louvain, csv_out="data/communities.csv")

    # Friend recommendations for top node
    if pr_top:
        example_user = pr_top[0][0]
        print(f"\nFriend recommendations for {example_user}:")
        for cand, score in recommend_friends(G, users, example_user, top_k=5):
            name_row = users.loc[users["_id"] == cand, "name"]
            name = name_row.iloc[0] if not name_row.empty else "Unknown"
            print(f"  {cand} ({name}) score={score}")

    # Subgraph example
    sub_g, sub_users = multi_attribute_subgraph(
        G, users,
        city='Mumbai',
        interests=['Music'],
        primaryLang='en'
    )
    print("\n--- SUBGRAPH (Mumbai + Music + en) ---")
    print("Nodes:", sub_g.number_of_nodes(), "Edges:", sub_g.number_of_edges())
    pr_sub, pr_top_sub = pagerank_with_names(sub_g, sub_users, top_k=5)
    detect_communities(sub_g, sub_users, community_louvain)
    if pr_top_sub:
        example_user = pr_top_sub[0][0]
        print(f"\nFriend recommendations (sub) for {example_user}:")
        for cand, score in recommend_friends(sub_g, sub_users, example_user, top_k=5):
            name_row = sub_users.loc[sub_users["_id"] == cand, "name"]
            name = name_row.iloc[0] if not name_row.empty else "Unknown"
            print(f"  {cand} ({name}) score={score}")
