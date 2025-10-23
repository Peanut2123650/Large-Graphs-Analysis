def adjusted_homophily(G, labels):
    num_edges = G.number_of_edges()
    edge_same = sum(
        1 for u, v in G.edges() if labels[u] == labels[v]
    )
    # Edge homophily
    h_edge = edge_same / num_edges

    # Degree-weighted probabilities
    degrees = dict(G.degree())
    label_degrees = {}
    for node, label in labels.items():
        label_degrees.setdefault(label, 0)
        label_degrees[label] += degrees[node]
    total_deg = sum(degrees.values())
    p_bar = {k: v / total_deg for k, v in label_degrees.items()}

    # Sum squared degree probabilities
    sum_pb2 = sum(v**2 for v in p_bar.values())

    # Adjusted homophily (assortativity coefficient)
    h_adj = (h_edge - sum_pb2) / (1 - sum_pb2)
    return h_adj

adj_homophily = adjusted_homophily(G, labels)
print(f"Adjusted Homophily: {adj_homophily:.3f}")
