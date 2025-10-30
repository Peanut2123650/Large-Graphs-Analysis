import networkx as nx
import pandas as pd
import matplotlib.pyplot as plt
from pyvis.network import Network
import colorsys
import os, webbrowser

# =====================
# 1. Load the Graph
# =====================
edges_path = r"C:\Users\p2123\Desktop\COLLEGE\PROJECT_3rdYear\Social_Network_Project\data\edges.csv"
edges_df = pd.read_csv(edges_path)

G = nx.Graph()
for _, row in edges_df.iterrows():
    G.add_edge(row["src"], row["dst"], weight=row.get("weight", 1))

print(f"Graph loaded: {G.number_of_nodes()} nodes, {G.number_of_edges()} edges")

# =====================
# 2. Compute centrality & choose top nodes
# =====================
degree_centrality = nx.degree_centrality(G)
sorted_nodes = sorted(degree_centrality.items(), key=lambda x: x[1], reverse=True)
top_nodes = [n for n, _ in sorted_nodes[:200]]  # focus on top 200 nodes
subG = G.subgraph(top_nodes).copy()

# =====================
# 3. Compute link prediction (Jaccard)
# =====================
preds = nx.jaccard_coefficient(G)
predicted_links = []

for u, v, p in preds:
    if u in subG and v in subG and not G.has_edge(u, v):
        predicted_links.append((u, v, p))

predicted_links = sorted(predicted_links, key=lambda x: x[2], reverse=True)[:50]  # top 50 only

# =====================
# 4. Static Visualization
# =====================
pos = nx.spring_layout(subG, k=0.15, iterations=20, seed=42)

plt.figure(figsize=(14, 10))
nx.draw_networkx_nodes(subG, pos, node_size=30, node_color="skyblue", alpha=0.8)
nx.draw_networkx_edges(subG, pos, edge_color="lightgray", width=0.5, alpha=0.6)

# Highlight predicted edges
for u, v, p in predicted_links:
    nx.draw_networkx_edges(subG, pos, edgelist=[(u, v)], edge_color="limegreen", style="dashed", width=2)

plt.title("Top 50 Predicted Links (Jaccard Coefficient) among Top 200 Nodes", fontsize=14)
plt.axis("off")
plt.tight_layout()
plt.show()

# =====================
# 5. Interactive Visualization
# =====================
net = Network(height="750px", width="100%", bgcolor="#ffffff", font_color="black")

# Add nodes
for node in subG.nodes():
    net.add_node(
        str(node),
        title=f"Node: {node}<br>Degree: {subG.degree(node)}",
        size=8 + subG.degree(node) * 0.7,
        color="skyblue"
    )

# Add existing edges
for u, v in subG.edges():
    net.add_edge(str(u), str(v), color="lightgray", width=0.5)

# Convert score to color
def score_to_color(score: float):
    hue = 0.22 + (0.35 - 0.22) * score
    rgb = colorsys.hsv_to_rgb(hue, 0.9, 1)
    r, g, b = [int(x * 255) for x in rgb]
    return f"rgba({r},{g},{b},{0.4 + 0.6 * score})"

# Add predicted edges
for u, v, score in predicted_links:
    if (str(u) in net.get_nodes()) and (str(v) in net.get_nodes()):
        net.add_edge(
            str(u),
            str(v),
            color=score_to_color(score),
            width=1.5 + 3 * score,
            dashes=True,
            title=f"<b>Predicted Link</b><br>{u} ↔ {v}<br>Score: {score:.4f}"
        )
    else:
        print(f"⚠️ Skipped invalid prediction: {u}, {v}")

# =====================
# 6. Configure Physics and Layout
# =====================
net.set_options("""
{
  "nodes": {
    "shape": "dot",
    "scaling": {"min": 5, "max": 20},
    "font": {"size": 12, "strokeWidth": 0}
  },
  "edges": {
    "smooth": false,
    "color": {"inherit": false},
    "width": 0.5
  },
  "physics": {
    "enabled": true,
    "solver": "forceAtlas2Based",
    "forceAtlas2Based": {
      "gravitationalConstant": -50,
      "centralGravity": 0.01,
      "springLength": 100,
      "springConstant": 0.08,
      "avoidOverlap": 0.3
    },
    "maxVelocity": 50,
    "minVelocity": 0.1,
    "timestep": 0.5,
    "stabilization": {"enabled": true, "iterations": 200}
  },
  "layout": {"improvedLayout": true},
  "interaction": {
    "hover": true,
    "tooltipDelay": 150,
    "navigationButtons": true,
    "zoomView": true
  }
}
""")

# =====================
# 7. Save and Inject JS (freeze after stabilization)
# =====================
output_html = r"C:\Users\p2123\Desktop\COLLEGE\PROJECT_3rdYear\Social_Network_Project\data\link_prediction_interactive.html"
net.save_graph(output_html)

# Append JS snippet to freeze layout
stabilize_js = """
<script type="text/javascript">
(function() {
  function whenReady(cb) {
    var tries = 0;
    var t = setInterval(function() {
      if (typeof network !== 'undefined') {
        clearInterval(t);
        cb();
      } else if (++tries > 200) clearInterval(t);
    }, 50);
  }
  whenReady(function() {
    try {
      network.once("stabilizationIterationsDone", function() {
        network.setOptions({ physics: { enabled: false } });
        console.log("Physics disabled after stabilization.");
      });
      setTimeout(function() {
        if (network.body.physics && network.body.physics.physicsEnabled()) {
          network.setOptions({ physics: { enabled: false } });
          console.log("Physics disabled by fallback timeout.");
        }
      }, 8000);
    } catch (e) {
      console.warn("Could not attach stabilization handler:", e);
    }
  });
})();
</script>
"""

try:
    with open(output_html, "r", encoding="utf-8") as f:
        html = f.read()
    html = html.replace("</body>", stabilize_js + "\n</body>") if "</body>" in html else html + stabilize_js
    with open(output_html, "w", encoding="utf-8") as f:
        f.write(html)
    print("✅ Appended stabilization JS to the HTML.")
except Exception as e:
    print("⚠️ Failed to append stabilization JS:", e)

# Open automatically
webbrowser.open(f"file://{os.path.abspath(output_html)}")

print("\n✅ Visualization saved successfully!")
print(" - Static: link_prediction_visualization.png (shown above)")
print(" - Interactive: link_prediction_interactive.html (auto-opened)")
