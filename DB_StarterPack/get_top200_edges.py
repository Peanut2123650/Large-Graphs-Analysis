import pandas as pd

# === Load Data ===
edges = pd.read_csv(
    r"C:\Users\p2123\Desktop\COLLEGE\PROJECT_3rdYear\Social_Network_Project\data\edges.csv"
)
top_nodes = pd.read_csv(
    r"C:\Users\p2123\Desktop\COLLEGE\PROJECT_3rdYear\Social_Network_Project\data\top_200_influential_nodes.csv"
)

# === Filter edges connecting only top 200 nodes ===
filtered_edges = edges[
    edges["src"].isin(top_nodes["Node"]) & edges["dst"].isin(top_nodes["Node"])
][["src", "dst"]]  # keep only 2 columns

# === Save for visualization ===
filtered_edges.to_csv(
    r"C:\Users\p2123\Desktop\COLLEGE\PROJECT_3rdYear\Social_Network_Project\data\top_200_edges.csv",
    index=False
)

print("✅ Saved 'top_200_edges.csv' (only src & dst) for visualization.")
print(f"Edges in top 200 subgraph: {len(filtered_edges)}")
