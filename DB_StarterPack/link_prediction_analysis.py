import networkx as nx
import pandas as pd
import matplotlib.pyplot as plt
import random
import os

# Paths (update if your folders differ)
DATA_PATH = "../data/edges.csv"
OUTPUT_IMG_PATH = "./images/link_prediction_visual.png"

def load_graph():
    print("Loading graph...")
    edges_df = pd.read_csv(DATA_PATH)
    G = nx.from_pandas_edgelist(edges_df, 'src', 'dst', edge_attr='weight', create_using=nx.Graph())
    print(f"Graph loaded: {G.number_of_nodes()} nodes, {G.number_of_edges()} edges")
    return G

def compute_link_prediction_scores(G):
    print("Computing link prediction scores...")

    # Compute several link prediction metrics
    preds_aa = nx.adamic_adar_index(G)
    preds_jc = nx.jaccard_coefficient(G)
    preds_pa = nx.preferential_attachment(G)

    df_list = []
    for pred_func, name in [(preds_aa, "Adamic-Adar"), (preds_jc, "Jaccard"), (preds_pa, "Preferential")]:
        temp = pd.DataFrame(pred_func, columns=["src", "dst", "score"])
        temp["method"] = name
        df_list.append(temp)

    results_df = pd.concat(df_list)
    results_df.sort_values("score", ascending=False, inplace=True)

    print("Top predicted links:")
    print(results_df.head())

    return results_df

def visualize_top_predictions(G, results_df, top_n=50):
    print(f"Visualizing top {top_n} predicted links...")

    top_preds = results_df.head(top_n)
    H = G.copy()
    for _, row in top_preds.iterrows():
        if not H.has_edge(row["src"], row["dst"]):
            H.add_edge(row["src"], row["dst"], color="red", weight=1.5)

    pos = nx.spring_layout(H, k=0.15, iterations=20)
    edge_colors = ["red" if "color" in d else "lightgray" for (u, v, d) in H.edges(data=True)]

    plt.figure(figsize=(12, 8))
    nx.draw(
        H,
        pos,
        node_size=30,
        node_color="skyblue",
        edge_color=edge_colors,
        with_labels=False,
        alpha=0.8
    )
    plt.title("Top Predicted Links (Red = Potential Future Connections)")
    plt.tight_layout()

    os.makedirs(os.path.dirname(OUTPUT_IMG_PATH), exist_ok=True)
    plt.savefig(OUTPUT_IMG_PATH)
    plt.show()

def main():
    G = load_graph()
    results_df = compute_link_prediction_scores(G)
    visualize_top_predictions(G, results_df)

if __name__ == "__main__":
    main()
