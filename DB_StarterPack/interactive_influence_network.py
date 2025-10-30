from pyvis.network import Network
import pandas as pd
import networkx as nx
import os

# Paths
DATA_PATH = "../data/edges.csv"
INFLUENCE_PATH = "../data/centrality_influence_scores.csv"
OUTPUT_PATH = "./images/interactive_influence_network.html"

# Load data
edges_df = pd.read_csv(DATA_PATH)
influence_df = pd.read_csv(INFLUENCE_PATH)

# Create undirected graph
G = nx.from_pandas_edgelist(edges_df, 'src', 'dst', edge_attr='weight', create_using=nx.Graph())

# Add influence scores
influence_dict = dict(zip(influence_df["node"].astype(str), influence_df["influence_weight"]))
nx.set_node_attributes(G, influence_dict, "influence_score")

# Select top nodes for visualization
top_nodes = sorted(influence_dict, key=influence_dict.get, reverse=True)[:300]  # top 300 for clarity
H = G.subgraph(top_nodes)

# Initialize interactive network
net = Network(
    height="750px",
    width="100%",
    bgcolor="#ffffff",
    font_color="black",
    notebook=False
)

# Add nodes with detailed tooltips
for node, data in H.nodes(data=True):
    influence = data.get("influence_score", 0)
    degree = H.degree(node)
    title = f"<b>Node:</b> {node}<br><b>Influence Score:</b> {influence:.4f}<br><b>Degree:</b> {degree}"
    net.add_node(
        node,
        label=str(node),
        title=title,
        value=influence,  # controls node size
        color="orange" if influence > 0.7 else "skyblue"
    )

# Add edges
for src, dst, data in H.edges(data=True):
    net.add_edge(src, dst, value=data.get("weight", 1))

# Add interactive controls
net.show_buttons(filter_=['physics'])

# Generate output HTML
os.makedirs(os.path.dirname(OUTPUT_PATH), exist_ok=True)
net.show(OUTPUT_PATH)

print(f"✅ Interactive network saved to {OUTPUT_PATH}")
print("💡 Open the HTML file in your browser to explore interactively.")
