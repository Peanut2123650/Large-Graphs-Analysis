import pandas as pd
import networkx as nx
from pyvis.network import Network
import os, io, webbrowser

# === Step 1: Load main graph data ===
edges_path = "../data/edges.csv"
users_path = "../data/users.csv"

try:
    edges_df = pd.read_csv(edges_path)
    users_df = pd.read_csv(users_path)
except FileNotFoundError:
    print("❌ Could not find edges.csv or users.csv. Please check paths.")
    exit()

G = nx.from_pandas_edgelist(edges_df, source="src", target="dst", edge_attr=True)

# === Step 2: Load highlight datasets ===
pagerank_path = "../data/pagerank.csv"
influence_path = "../data/centrality_influence_scores.csv"
homophily_path = "../data/homophily_top_influencers.csv"

try:
    pagerank_df = pd.read_csv(pagerank_path)
    influence_df = pd.read_csv(influence_path)
    homophily_df = pd.read_csv(homophily_path)
except Exception as e:
    print("❌ Error loading highlight files:", e)
    exit()

# === Step 3: Collect all special nodes ===
highlight_nodes = set()

def extract_nodes(df):
    for col in ["node", "Node", "id", "ID"]:
        if col in df.columns:
            return df[col].astype(str)
    return []

highlight_nodes.update(extract_nodes(pagerank_df))
highlight_nodes.update(extract_nodes(influence_df))
highlight_nodes.update(extract_nodes(homophily_df))

print(f"✅ Total highlighted nodes: {len(highlight_nodes)}")

# === Step 4: Create PyVis network (stable layout) ===
net = Network(
    notebook=False,
    cdn_resources="remote",
    height="850px",
    width="100%",
    bgcolor="#111111",
    font_color="white"
)

# Use a force-directed layout with limited movement
net.set_options("""
{
  "nodes": {
    "shape": "dot",
    "font": {"size": 12, "strokeWidth": 0}
  },
  "edges": {
    "color": {"inherit": false},
    "smooth": {"type": "continuous"},
    "width": 0.5
  },
  "physics": {
    "enabled": true,
    "solver": "barnesHut",
    "stabilization": {
      "enabled": true,
      "iterations": 250,
      "updateInterval": 25
    },
    "barnesHut": {
      "gravitationalConstant": -30000,
      "springLength": 180,
      "springConstant": 0.02,
      "damping": 0.2,
      "avoidOverlap": 0.6
    }
  },
  "interaction": {
    "hover": true,
    "zoomView": true,
    "dragNodes": true,
    "navigationButtons": true
  }
}
""")

# === Step 5: Add nodes and edges ===
for node in G.nodes():
    node_id = str(node)
    degree = G.degree(node)

    if node_id in highlight_nodes:
        color = "#FF0000"
        size = 25
    else:
        color = "skyblue"
        size = 8 + degree * 0.4

    net.add_node(
        node_id,
        title=f"<b>Node:</b> {node_id}<br>Degree: {degree}",
        color=color,
        size=size
    )

for u, v, data in G.edges(data=True):
    net.add_edge(str(u), str(v), color="gray", width=0.8)

# === Step 6: Add floating legend ===
legend_html = """
<div style="
  position: fixed; bottom: 20px; right: 20px;
  background: rgba(255,255,255,0.85);
  border: 1px solid #aaa; border-radius: 8px;
  padding: 10px 14px;
  font-family: Arial, sans-serif; font-size: 13px;
  box-shadow: 0 2px 5px rgba(0,0,0,0.3);
  z-index: 9999;">
  <b>Legend</b><br>
  <div style="margin-top: 4px;">
    <span style="display:inline-block; width:14px; height:14px; background:#FF0000; border-radius:50%; margin-right:6px;"></span>
    Highlighted Nodes (Important / Influential)
  </div>
  <div style="margin-top: 4px;">
    <span style="display:inline-block; width:14px; height:14px; background:skyblue; border-radius:50%; margin-right:6px;"></span>
    Regular Nodes
  </div>
</div>
"""

output_path = os.path.abspath("../data/highlighted_nodes_network.html")

# === Step 7: Save and open ===
try:
    html_content = net.generate_html()
    html_content = html_content.replace("</body>", legend_html + "\n</body>")

    with io.open(output_path, mode="w", encoding="utf-8") as f:
        f.write(html_content)

    print(f"✅ Visualization created successfully → {output_path}")
    webbrowser.open(f"file://{output_path}")

except Exception as e:
    print(f"❌ Error saving visualization: {e}")
