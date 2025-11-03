import streamlit as st
import networkx as nx
import plotly.graph_objects as go
import pandas as pd
from io import StringIO

# Page configuration
st.set_page_config(page_title="Social Network Graph", layout="wide")

# Initialize session state
if 'graph' not in st.session_state:
    # Sample data
    edges = [
        ('Alice', 'Bob'), ('Alice', 'Carol'), ('Bob', 'David'),
        ('Carol', 'David'), ('David', 'Eve'), ('Eve', 'Frank'),
        ('Frank', 'Grace'), ('Grace', 'Henry'), ('Bob', 'Frank')
    ]
    st.session_state.graph = nx.Graph()
    st.session_state.graph.add_edges_from(edges)
    st.session_state.original_graph = st.session_state.graph.copy()

if 'highlighted_nodes' not in st.session_state:
    st.session_state.highlighted_nodes = set()

if 'hop_nodes' not in st.session_state:
    st.session_state.hop_nodes = set()
    st.session_state.hop_edges = set()

if 'remove_mode' not in st.session_state:
    st.session_state.remove_mode = None

# Title
st.title("🔗 Interactive Social Network Graph")

# Sidebar controls
st.sidebar.header("Controls")

# File upload
uploaded_file = st.sidebar.file_uploader("Upload CSV (source,target)", type=['csv'])
if uploaded_file is not None:
    try:
        df = pd.read_csv(uploaded_file)
        if len(df.columns) >= 2:
            edges = list(zip(df.iloc[:, 0], df.iloc[:, 1]))
            st.session_state.graph = nx.Graph()
            st.session_state.graph.add_edges_from(edges)
            st.session_state.original_graph = st.session_state.graph.copy()
            st.sidebar.success("CSV loaded successfully!")
    except Exception as e:
        st.sidebar.error(f"Error loading CSV: {e}")

# Reset button
if st.sidebar.button("🔄 Reset Graph", use_container_width=True):
    st.session_state.graph = st.session_state.original_graph.copy()
    st.session_state.highlighted_nodes = set()
    st.session_state.hop_nodes = set()
    st.session_state.hop_edges = set()
    st.session_state.remove_mode = None
    st.rerun()

st.sidebar.divider()

# Node removal
st.sidebar.subheader("Remove Elements")
remove_node = st.sidebar.selectbox(
    "Select node to remove:",
    ["None"] + sorted(list(st.session_state.graph.nodes())),
    key="remove_node_select"
)
if remove_node != "None" and st.sidebar.button("🗑️ Remove Node", use_container_width=True):
    if remove_node in st.session_state.graph.nodes():
        st.session_state.graph.remove_node(remove_node)
        st.rerun()

# Edge removal
edges_list = ["None"] + [f"{u} - {v}" for u, v in st.session_state.graph.edges()]
remove_edge = st.sidebar.selectbox(
    "Select edge to remove:",
    edges_list,
    key="remove_edge_select"
)
if remove_edge != "None" and st.sidebar.button("🗑️ Remove Edge", use_container_width=True):
    u, v = remove_edge.split(" - ")
    if st.session_state.graph.has_edge(u, v):
        st.session_state.graph.remove_edge(u, v)
        st.rerun()

st.sidebar.divider()

# Highlight nodes
st.sidebar.subheader("⭐ Highlight Nodes")
highlight_input = st.sidebar.text_input(
    "Enter node names (comma-separated):",
    placeholder="e.g., Alice,Bob,Carol"
)
if st.sidebar.button("Highlight Nodes", use_container_width=True):
    nodes = [n.strip() for n in highlight_input.split(',') if n.strip()]
    st.session_state.highlighted_nodes = set(nodes) & set(st.session_state.graph.nodes())
    st.session_state.hop_nodes = set()
    st.session_state.hop_edges = set()
    st.rerun()

st.sidebar.divider()

# K-hop visualization
st.sidebar.subheader("📡 Show K-Hops")
hop_source = st.sidebar.selectbox(
    "Source node:",
    ["None"] + sorted(list(st.session_state.graph.nodes())),
    key="hop_source"
)
hop_distance = st.sidebar.number_input("Number of hops (k):", min_value=1, max_value=10, value=2)

if st.sidebar.button("Show K-Hops", use_container_width=True):
    if hop_source != "None" and hop_source in st.session_state.graph.nodes():
        # BFS to find k-hop neighbors
        reachable_nodes = {hop_source}
        reachable_edges = set()
        
        current_level = [hop_source]
        for _ in range(hop_distance):
            next_level = []
            for node in current_level:
                for neighbor in st.session_state.graph.neighbors(node):
                    if neighbor not in reachable_nodes:
                        reachable_nodes.add(neighbor)
                        next_level.append(neighbor)
                        reachable_edges.add((min(node, neighbor), max(node, neighbor)))
            current_level = next_level
        
        st.session_state.hop_nodes = reachable_nodes
        st.session_state.hop_edges = reachable_edges
        st.session_state.highlighted_nodes = set()
        st.rerun()

# Clear highlights
if st.sidebar.button("Clear All Highlights", use_container_width=True):
    st.session_state.highlighted_nodes = set()
    st.session_state.hop_nodes = set()
    st.session_state.hop_edges = set()
    st.rerun()

# Create graph visualization
def create_network_graph(G):
    # Use spring layout for better node distribution
    pos = nx.spring_layout(G, k=2, iterations=50, seed=42)
    
    # Create edge traces
    edge_trace = []
    
    for edge in G.edges():
        x0, y0 = pos[edge[0]]
        x1, y1 = pos[edge[1]]
        
        # Check if edge is in hop visualization
        edge_tuple = (min(edge[0], edge[1]), max(edge[0], edge[1]))
        is_hop_edge = edge_tuple in st.session_state.hop_edges
        
        edge_trace.append(
            go.Scatter(
                x=[x0, x1, None],
                y=[y0, y1, None],
                mode='lines',
                line=dict(
                    width=5 if is_hop_edge else 3,
                    color='#10b981' if is_hop_edge else '#475569'
                ),
                hoverinfo='none',
                showlegend=False
            )
        )
    
    # Create node trace
    node_x = []
    node_y = []
    node_text = []
    node_colors = []
    node_sizes = []
    
    for node in G.nodes():
        x, y = pos[node]
        node_x.append(x)
        node_y.append(y)
        node_text.append(node)
        
        # Determine node color and size
        if node in st.session_state.hop_nodes:
            node_colors.append('#10b981')  # Green for hop nodes
            node_sizes.append(25)
        elif node in st.session_state.highlighted_nodes:
            node_colors.append('#f59e0b')  # Orange for highlighted
            node_sizes.append(30)
        else:
            node_colors.append('#3b82f6')  # Blue default
            node_sizes.append(20)
    
    node_trace = go.Scatter(
        x=node_x,
        y=node_y,
        mode='markers+text',
        marker=dict(
            size=node_sizes,
            color=node_colors,
            line=dict(width=2, color='#1e40af')
        ),
        text=node_text,
        textposition="top center",
        textfont=dict(size=12, color='#1e293b', family='Arial Black'),
        hoverinfo='text',
        hovertext=node_text,
        showlegend=False
    )
    
    # Create figure
    fig = go.Figure(data=edge_trace + [node_trace])
    
    fig.update_layout(
        showlegend=False,
        hovermode='closest',
        margin=dict(b=0, l=0, r=0, t=0),
        xaxis=dict(showgrid=False, zeroline=False, showticklabels=False),
        yaxis=dict(showgrid=False, zeroline=False, showticklabels=False),
        plot_bgcolor='white',
        height=700
    )
    
    return fig

# Display the graph
if len(st.session_state.graph.nodes()) > 0:
    fig = create_network_graph(st.session_state.graph)
    st.plotly_chart(fig, use_container_width=True)
    
    # Display graph statistics
    col1, col2, col3, col4 = st.columns(4)
    with col1:
        st.metric("Nodes", len(st.session_state.graph.nodes()))
    with col2:
        st.metric("Edges", len(st.session_state.graph.edges()))
    with col3:
        st.metric("Highlighted", len(st.session_state.highlighted_nodes))
    with col4:
        st.metric("K-Hop Nodes", len(st.session_state.hop_nodes))
else:
    st.warning("Graph is empty. Please reset or upload a CSV file.")

# Instructions
with st.expander("📖 Instructions"):
    st.markdown("""
    **How to use this application:**
    
    1. **Upload CSV**: Upload a CSV file with two columns (source, target) to create your own network
    2. **Remove Elements**: Select nodes or edges from the dropdown menus and click remove
    3. **Highlight Nodes**: Enter comma-separated node names to highlight and enlarge them (orange)
    4. **Show K-Hops**: Select a source node and number of hops to visualize information spread (green)
    5. **Reset Graph**: Click the reset button to restore the original graph
    6. **Clear Highlights**: Remove all highlighting to see the default view
    
    **Color Legend:**
    - 🔵 Blue: Default nodes
    - 🟠 Orange: Highlighted nodes (enlarged)
    - 🟢 Green: K-hop reachable nodes and edges
    """)
