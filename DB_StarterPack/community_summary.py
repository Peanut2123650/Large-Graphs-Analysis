import pandas as pd
import networkx as nx
import community as community_louvain     # python-louvain

EDGES = r"C:\Users\p2123\Desktop\COLLEGE\PROJECT_3rdYear\Social_Network_Project\data\edges.csv"
LEADERS = "per_community_leaders.csv"


def load_graph():
    df = pd.read_csv(EDGES)
    if {"source", "target"}.issubset(df.columns):
        edges = df[["source", "target"]].values.tolist()
    else:
        edges = df.iloc[:, :2].values.tolist()

    G = nx.Graph()
    G.add_edges_from(edges)
    return G


def main():
    print("\n=== Community Summary Report ===\n")

    # Load graph
    G = load_graph()
    print(f"Graph loaded: {G.number_of_nodes()} nodes, {G.number_of_edges()} edges\n")

    # Louvain detection
    partition = community_louvain.best_partition(G)

    # 1️⃣ Number of communities
    communities = set(partition.values())
    print(f"Number of communities: {len(communities)}")

    # 2️⃣ Modularity score
    modularity = community_louvain.modularity(partition, G)
    print(f"Modularity score: {modularity:.4f}")

    # 3️⃣ Community size distribution
    comm_sizes = {}
    for node, comm in partition.items():
        comm_sizes[comm] = comm_sizes.get(comm, 0) + 1

    size_df = pd.DataFrame({
        "community": list(comm_sizes.keys()),
        "size": list(comm_sizes.values())
    }).sort_values("size", ascending=False)

    print("\nCommunity size distribution:")
    print(size_df)

    size_df.to_csv("community_size_distribution.csv", index=False)
    print("\nSaved: community_size_distribution.csv")

    # 4️⃣ Leader nodes per community (already computed earlier)
    try:
        leaders_df = pd.read_csv(LEADERS)
        print("\nLeader nodes per community:")
        print(leaders_df)
    except:
        print("\nCould not find per_community_leaders.csv — run select_leader_deputy.py first.")

    print("\n=== Report Complete ===\n")


if __name__ == "__main__":
    main()
