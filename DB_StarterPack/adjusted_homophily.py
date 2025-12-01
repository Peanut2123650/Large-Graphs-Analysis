# adjusted_homophily.py
# Computes global adjusted homophily safely for large graphs.

def adjusted_homophily(G, labels):
    """
    Computes adjusted homophily (assortativity-style) for a graph.

    Parameters
    ----------
    G : networkx.Graph or networkx.DiGraph
        Graph containing nodes.
    labels : dict
        Mapping: node -> label (e.g., community index).

    Returns
    -------
    float : adjusted homophily value in [-1, 1]
    """
    # Defensive: ensure missing labels don't crash
    L = {n: labels.get(n, None) for n in G.nodes()}

    # Count edges where both endpoints share the same label
    total_edges = 0
    same_label_edges = 0

    for u, v in G.edges():
        total_edges += 1
        if L[u] is not None and L[u] == L[v]:
            same_label_edges += 1

    if total_edges == 0:
        return 0.0

    h_edge = same_label_edges / total_edges

    # Degree-based expected probability
    degrees = dict(G.degree())
    total_deg = sum(degrees.values())

    if total_deg == 0:
        return 0.0

    # Sum of degrees for each label
    label_deg = {}
    for n, deg in degrees.items():
        lab = L[n]
        if lab is None:
            continue
        label_deg[lab] = label_deg.get(lab, 0) + deg

    # Degree proportions
    p_bar = {lab: d / total_deg for lab, d in label_deg.items()}

    # Sum of squared degree proportions
    sum_pb2 = sum(p * p for p in p_bar.values())

    # If denominator is zero, avoid crash
    if (1 - sum_pb2) == 0:
        return 0.0

    # Adjusted homophily formula
    h_adj = (h_edge - sum_pb2) / (1 - sum_pb2)

    return h_adj


# Example run (only works if G and labels already exist in the script using this module)
# adj_homophily = adjusted_homophily(G, labels)
# print(f"Adjusted Homophily: {adj_homophily:.3f}")

