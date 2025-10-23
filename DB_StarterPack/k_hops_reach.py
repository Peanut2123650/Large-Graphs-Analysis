import pandas as pd
import networkx as nx

# Load the graph
edge_df = pd.read_csv('edge.csv')
G = nx.from_pandas_edgelist(edge_df, source='src', target='dst')

def k_hop_nodes(G, node, k):
    node = int(node)
    visited = set([node])
    frontier = set([node])
    for _ in range(k):
        next_frontier = set()
        for curr_node in frontier:
            next_frontier.update(G.neighbors(curr_node))
        next_frontier = next_frontier - visited
        visited.update(next_frontier)
        frontier = next_frontier
    return visited

# Process for all nodes
k = int(input("Enter the number of hops (k): "))

for node in G.nodes():
    reachable_nodes = k_hop_nodes(G, node, k)
    print(f"Node {node}: {len(reachable_nodes)} nodes within {k} hops.")

