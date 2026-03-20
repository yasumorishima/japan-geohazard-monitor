"""Graph Neural Network for spatiotemporal earthquake prediction — Colab GPU training.

Architecture:
    Input: (batch, T=30, N=121, C) — 30 steps × 3 days = 90 days history, N=11×11 grid nodes
    → Per-node temporal encoding (GRU over T steps)
    → Graph convolution (GATv2Conv × 3 layers) — message passing over spatial graph
    → Node-level classification (MLP → sigmoid)
    Output: (batch, N) — per-cell probability

Graph structure:
    - Nodes: 11×11 = 121 grid cells (26-46°N, 128-148°E, 2° spacing)
    - Edges: 8-connectivity (spatial adjacency) + same-tectonic-zone connections
    - Edge features: distance, CFS gradient direction, zone membership

Why GNN over ConvLSTM:
    - ConvLSTM treats the grid as a regular image — but Japan's seismogenic zones are
      irregularly shaped (Nankai trough, Japan Trench, Izu-Bonin arc)
    - GNN captures fault-network topology: stress transfer along plate boundaries,
      not just Euclidean proximity
    - Message passing naturally models Coulomb stress cascading across fault segments
    - Graph attention learns which neighbors matter (subduction interface vs stable crust)

Physical basis:
    Earthquake triggering is fundamentally a network phenomenon. Coulomb stress from
    one rupture loads adjacent faults (Stein 1999, Toda et al. 2005), creating cascade
    pathways that follow tectonic structure, not Euclidean distance. A GNN's message
    passing is a direct computational analog of stress transfer.

References:
    - SeismoQuakeGNN (Frontiers in AI, 2025) — GNN+Transformer hybrid
    - Veličković et al. (2018) GAT — Graph Attention Networks
    - Bruna et al. (2023) GATv2 — improved attention mechanism
    - Stein (1999) Nature 402 — The role of stress transfer in earthquake occurrence
    - DeVries et al. (2018) Nature 560 — Deep learning of aftershock patterns
"""

import json
import math
import os
import sys
import time
from pathlib import Path

try:
    import torch
    import torch.nn as nn
    import torch.optim as optim
    from torch.utils.data import Dataset, DataLoader
    HAS_TORCH = True
except ImportError:
    HAS_TORCH = False
    print("PyTorch not available. Install with: pip install torch")

try:
    import numpy as np
    HAS_NUMPY = True
except ImportError:
    HAS_NUMPY = False

try:
    from torch_geometric.nn import GATv2Conv, global_mean_pool
    from torch_geometric.data import Data, Batch
    HAS_PYG = True
except ImportError:
    HAS_PYG = False
    print("PyTorch Geometric not available. Install with: pip install torch-geometric")


# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

SEQUENCE_LENGTH = 30   # 30 time steps × 3 days = 90 days history
POS_WEIGHT = 20.0      # BCE positive class weight
LEARNING_RATE = 5e-4
WEIGHT_DECAY = 1e-4
BATCH_SIZE = 16        # Smaller than ConvLSTM (graph batching is heavier)
MAX_EPOCHS = 50
PATIENCE = 10
GNN_HIDDEN = 64
GNN_HEADS = 4          # Multi-head attention
GNN_LAYERS = 3
GRU_HIDDEN = 64
DROPOUT = 0.2

# Grid definition (must match export_feature_matrix.py)
GRID_H, GRID_W = 11, 11
GRID_LAT_MIN, GRID_LAT_MAX = 26, 46
GRID_LON_MIN, GRID_LON_MAX = 128, 148
CELL_SIZE = 2.0

# Drive paths (Colab)
DRIVE_BASE = "/content/drive/MyDrive/kaggle/geohazard"
FEATURE_MATRIX_PATH = os.path.join(DRIVE_BASE, "feature_matrix.json")
RESULTS_PATH = os.path.join(DRIVE_BASE, "gnn_results.json")
CHECKPOINT_DIR = os.path.join(DRIVE_BASE, "checkpoints_gnn")

# Japan tectonic zones (from src/physics.py)
TECTONIC_ZONES = {
    "tohoku_offshore": {"lat_min": 35, "lat_max": 42, "lon_min": 140, "lon_max": 148},
    "kanto_tokai":     {"lat_min": 33, "lat_max": 36, "lon_min": 137, "lon_max": 142},
    "nankai":          {"lat_min": 30, "lat_max": 35, "lon_min": 130, "lon_max": 137},
    "kyushu":          {"lat_min": 28, "lat_max": 34, "lon_min": 128, "lon_max": 133},
    "hokkaido":        {"lat_min": 41, "lat_max": 46, "lon_min": 140, "lon_max": 148},
    "izu_bonin":       {"lat_min": 26, "lat_max": 35, "lon_min": 138, "lon_max": 145},
}


# ---------------------------------------------------------------------------
# Graph Construction
# ---------------------------------------------------------------------------

def classify_zone(lat, lon):
    """Classify grid cell into tectonic zone."""
    for name, bounds in TECTONIC_ZONES.items():
        if (bounds["lat_min"] <= lat <= bounds["lat_max"] and
                bounds["lon_min"] <= lon <= bounds["lon_max"]):
            return name
    return "other"


def build_graph():
    """Build static graph structure for Japan seismic grid.

    Returns:
        edge_index: (2, E) tensor of edge indices
        edge_attr: (E, D_edge) tensor of edge features
        node_coords: (N, 2) lat/lon coordinates
        node_zones: list of zone names per node
    """
    # Generate node coordinates
    lats = np.arange(GRID_LAT_MIN, GRID_LAT_MAX + CELL_SIZE, CELL_SIZE)
    lons = np.arange(GRID_LON_MIN, GRID_LON_MAX + CELL_SIZE, CELL_SIZE)
    node_coords = []
    node_zones = []
    coord_to_idx = {}

    for i, lat in enumerate(lats):
        for j, lon in enumerate(lons):
            idx = i * len(lons) + j
            node_coords.append([lat, lon])
            node_zones.append(classify_zone(lat, lon))
            coord_to_idx[(i, j)] = idx

    N = len(node_coords)
    node_coords = np.array(node_coords, dtype=np.float32)

    # Build edges
    src_list, dst_list, edge_feats = [], [], []

    for i in range(len(lats)):
        for j in range(len(lons)):
            src_idx = coord_to_idx[(i, j)]
            src_lat, src_lon = lats[i], lons[j]
            src_zone = node_zones[src_idx]

            # Type 1: 8-connectivity spatial adjacency
            for di in (-1, 0, 1):
                for dj in (-1, 0, 1):
                    if di == 0 and dj == 0:
                        continue
                    ni, nj = i + di, j + dj
                    if (ni, nj) not in coord_to_idx:
                        continue
                    dst_idx = coord_to_idx[(ni, nj)]
                    dst_lat, dst_lon = lats[ni], lons[nj]
                    dst_zone = node_zones[dst_idx]

                    # Edge features
                    dist = math.sqrt((di * CELL_SIZE) ** 2 + (dj * CELL_SIZE) ** 2)
                    inv_dist = 1.0 / dist
                    same_zone = 1.0 if src_zone == dst_zone else 0.0
                    # Direction encoding (sin/cos of angle)
                    angle = math.atan2(dj, di)
                    dir_sin = math.sin(angle)
                    dir_cos = math.cos(angle)

                    src_list.append(src_idx)
                    dst_list.append(dst_idx)
                    edge_feats.append([inv_dist, same_zone, dir_sin, dir_cos])

            # Type 2: Same-zone connections (non-adjacent cells in same tectonic zone)
            if src_zone != "other":
                for k in range(N):
                    if k == src_idx:
                        continue
                    if node_zones[k] == src_zone:
                        ki, kj = k // len(lons), k % len(lons)
                        dist_i = abs(i - ki)
                        dist_j = abs(j - kj)
                        # Skip already-connected 8-neighbors
                        if dist_i <= 1 and dist_j <= 1:
                            continue
                        # Only connect within 3-cell radius to limit edge count
                        if dist_i > 3 or dist_j > 3:
                            continue
                        dist = math.sqrt((dist_i * CELL_SIZE) ** 2 + (dist_j * CELL_SIZE) ** 2)
                        inv_dist = 1.0 / dist
                        angle = math.atan2(dist_j, dist_i)

                        src_list.append(src_idx)
                        dst_list.append(k)
                        edge_feats.append([inv_dist, 1.0, math.sin(angle), math.cos(angle)])

    edge_index = torch.tensor([src_list, dst_list], dtype=torch.long)
    edge_attr = torch.tensor(edge_feats, dtype=torch.float32)

    print(f"Graph: {N} nodes, {edge_index.shape[1]} edges "
          f"({edge_index.shape[1] / N:.1f} avg degree)")

    # Zone distribution
    zone_counts = {}
    for z in node_zones:
        zone_counts[z] = zone_counts.get(z, 0) + 1
    for z, c in sorted(zone_counts.items()):
        print(f"  Zone {z}: {c} nodes")

    return edge_index, edge_attr, node_coords, node_zones


# ---------------------------------------------------------------------------
# Temporal Node Encoder (GRU per node)
# ---------------------------------------------------------------------------

class TemporalNodeEncoder(nn.Module):
    """Per-node GRU encoding of temporal feature sequences."""

    def __init__(self, n_features, hidden_size=GRU_HIDDEN):
        super().__init__()
        self.gru = nn.GRU(
            input_size=n_features,
            hidden_size=hidden_size,
            num_layers=2,
            batch_first=True,
            dropout=DROPOUT,
        )
        self.norm = nn.LayerNorm(hidden_size)

    def forward(self, x):
        """
        Args:
            x: (batch, T, N, C) — temporal node features

        Returns:
            (batch, N, hidden) — per-node temporal embeddings
        """
        B, T, N, C = x.shape
        # Reshape to (B*N, T, C) for GRU
        x = x.permute(0, 2, 1, 3).contiguous().view(B * N, T, C)
        _, h_n = self.gru(x)  # h_n: (2, B*N, hidden)
        h = h_n[-1]  # Last layer: (B*N, hidden)
        h = h.view(B, N, -1)
        return self.norm(h)


# ---------------------------------------------------------------------------
# GNN Model
# ---------------------------------------------------------------------------

class SeismoGNN(nn.Module):
    """Graph Attention Network for earthquake prediction.

    Architecture:
        1. Temporal encoding: GRU over T time steps per node
        2. Graph attention: GATv2Conv × 3 layers with residual connections
        3. Node classification: MLP → sigmoid per node
    """

    def __init__(self, n_features, n_edge_features=4,
                 hidden=GNN_HIDDEN, heads=GNN_HEADS, n_layers=GNN_LAYERS):
        super().__init__()

        # Temporal encoder
        self.temporal = TemporalNodeEncoder(n_features, hidden)

        # Graph attention layers with edge features
        self.gat_layers = nn.ModuleList()
        self.gat_norms = nn.ModuleList()

        for i in range(n_layers):
            in_ch = hidden if i == 0 else hidden * heads
            self.gat_layers.append(
                GATv2Conv(
                    in_channels=in_ch,
                    out_channels=hidden,
                    heads=heads,
                    edge_dim=n_edge_features,
                    dropout=DROPOUT,
                    concat=True,  # Output: hidden * heads
                )
            )
            self.gat_norms.append(nn.LayerNorm(hidden * heads))

        # Residual projection for skip connections
        self.skip_proj = nn.Linear(hidden, hidden * heads)

        # Output MLP
        self.classifier = nn.Sequential(
            nn.Linear(hidden * heads, hidden),
            nn.ReLU(inplace=True),
            nn.Dropout(DROPOUT),
            nn.Linear(hidden, 1),
        )

    def forward(self, x, edge_index, edge_attr):
        """
        Args:
            x: (batch, T, N, C) — temporal node features
            edge_index: (2, E) — graph edges
            edge_attr: (E, D_edge) — edge features

        Returns:
            (batch, N) — per-node probability
        """
        B, T, N, C = x.shape

        # 1. Temporal encoding: (B, N, hidden)
        h = self.temporal(x)

        # 2. Graph attention layers (process each sample in batch)
        outputs = []
        for b in range(B):
            node_feat = h[b]  # (N, hidden)

            # Skip connection anchor
            skip = self.skip_proj(node_feat)

            for i, (gat, norm) in enumerate(zip(self.gat_layers, self.gat_norms)):
                node_feat_new = gat(node_feat, edge_index, edge_attr)
                node_feat_new = norm(node_feat_new)
                node_feat_new = torch.relu(node_feat_new)

                # Residual connection (from layer 1 onward)
                if i > 0 and node_feat_new.shape == node_feat.shape:
                    node_feat_new = node_feat_new + node_feat
                elif i == 0:
                    node_feat_new = node_feat_new + skip

                node_feat = node_feat_new

            # 3. Node classification
            logits = self.classifier(node_feat).squeeze(-1)  # (N,)
            probs = torch.sigmoid(logits)
            outputs.append(probs)

        return torch.stack(outputs)  # (B, N)


# ---------------------------------------------------------------------------
# Dataset
# ---------------------------------------------------------------------------

class GraphEarthquakeDataset(Dataset):
    """Earthquake dataset for GNN (graph-structured)."""

    def __init__(self, features, labels, times, seq_length=SEQUENCE_LENGTH):
        """
        Args:
            features: (n_timesteps, H, W, C) numpy array
            labels: (n_timesteps, H, W) numpy array
            times: (n_timesteps,) array of t_days
        """
        self.seq_length = seq_length
        H, W = features.shape[1], features.shape[2]
        N = H * W

        # Reshape spatial grid to node list: (T, N, C) and (T, N)
        self.features = features.reshape(features.shape[0], N, features.shape[3])
        self.labels = labels.reshape(labels.shape[0], N)
        self.times = times

        self.valid_indices = list(range(seq_length, len(features)))

    def __len__(self):
        return len(self.valid_indices)

    def __getitem__(self, idx):
        t = self.valid_indices[idx]
        # (T, N, C) sequence
        x = self.features[t - self.seq_length:t]
        y = self.labels[t]  # (N,)
        return (
            torch.FloatTensor(x),
            torch.FloatTensor(y),
            self.times[t],
        )


# ---------------------------------------------------------------------------
# Training
# ---------------------------------------------------------------------------

def train_one_epoch(model, loader, optimizer, edge_index, edge_attr, device):
    """Train for one epoch."""
    model.train()
    total_loss = 0
    n_batches = 0

    for x, y, _ in loader:
        x, y = x.to(device), y.to(device)
        ei = edge_index.to(device)
        ea = edge_attr.to(device)

        optimizer.zero_grad()
        pred = model(x, ei, ea)

        # Weighted BCE
        weight = torch.where(y >= 0.5, POS_WEIGHT, 1.0)
        loss = nn.functional.binary_cross_entropy(pred, y, reduction='none')
        loss = (loss * weight).mean()

        loss.backward()
        torch.nn.utils.clip_grad_norm_(model.parameters(), max_norm=1.0)
        optimizer.step()

        total_loss += loss.item()
        n_batches += 1

    return total_loss / max(n_batches, 1)


def evaluate(model, loader, edge_index, edge_attr, device):
    """Evaluate model, return AUC."""
    model.eval()
    all_probs = []
    all_labels = []

    with torch.no_grad():
        for x, y, _ in loader:
            x = x.to(device)
            ei = edge_index.to(device)
            ea = edge_attr.to(device)
            pred = model(x, ei, ea)
            all_probs.extend(pred.cpu().numpy().flatten().tolist())
            all_labels.extend(y.numpy().flatten().tolist())

    auc = compute_auc(all_labels, all_probs)
    return auc, all_probs, all_labels


def compute_auc(y_true, y_prob):
    """Compute AUC-ROC (pure Python, same as ConvLSTM)."""
    n_pos = sum(1 for y in y_true if y >= 0.5)
    n_neg = len(y_true) - n_pos
    if n_pos == 0 or n_neg == 0:
        return 0.5

    combined = sorted(zip(y_prob, y_true), key=lambda x: -x[0])
    tp, fp = 0, 0
    prev_fpr, prev_tpr = 0, 0
    auc = 0

    for prob, label in combined:
        if label >= 0.5:
            tp += 1
        else:
            fp += 1
        tpr = tp / n_pos
        fpr = fp / n_neg
        auc += (fpr - prev_fpr) * (tpr + prev_tpr) / 2
        prev_fpr = fpr
        prev_tpr = tpr

    return auc


# ---------------------------------------------------------------------------
# Walk-Forward CV
# ---------------------------------------------------------------------------

def walk_forward_cv(features, labels, times, n_features, edge_index, edge_attr,
                    initial_train_years=5, step_years=1, test_years=1):
    """Walk-forward cross-validation for GNN."""
    device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
    print(f"Device: {device}")

    day_min = times[0]
    day_max = times[-1]
    initial_train_days = initial_train_years * 365.25
    step_days = step_years * 365.25
    test_days = test_years * 365.25

    splits = []
    test_start = day_min + initial_train_days
    while test_start + test_days <= day_max:
        splits.append((day_min, test_start, test_start, test_start + test_days))
        test_start += step_days

    print(f"Walk-forward CV: {len(splits)} folds")

    fold_results = []
    all_test_probs = []
    all_test_labels = []

    for fold_idx, (train_start, train_end, t_start, t_end) in enumerate(splits):
        print(f"\n--- Fold {fold_idx} ---")

        # Create datasets with time filtering
        train_dataset = GraphEarthquakeDataset(features, labels, times, SEQUENCE_LENGTH)
        test_dataset = GraphEarthquakeDataset(features, labels, times, SEQUENCE_LENGTH)

        train_dataset.valid_indices = [
            i for i in train_dataset.valid_indices
            if train_start <= times[i] < train_end
        ]
        test_dataset.valid_indices = [
            i for i in test_dataset.valid_indices
            if t_start <= times[i] < t_end
        ]

        n_pos_train = sum(labels.reshape(labels.shape[0], -1)[i].sum()
                          for i in train_dataset.valid_indices)
        n_pos_test = sum(labels.reshape(labels.shape[0], -1)[i].sum()
                         for i in test_dataset.valid_indices)
        print(f"  Train: {len(train_dataset)} samples (pos={n_pos_train:.0f})")
        print(f"  Test: {len(test_dataset)} samples (pos={n_pos_test:.0f})")

        if n_pos_train < 5 or len(test_dataset) < 50:
            print(f"  Skipping fold {fold_idx}: insufficient data")
            continue

        train_loader = DataLoader(train_dataset, batch_size=BATCH_SIZE, shuffle=True)
        test_loader = DataLoader(test_dataset, batch_size=BATCH_SIZE)

        # Model
        model = SeismoGNN(n_features=n_features).to(device)
        optimizer = optim.AdamW(model.parameters(), lr=LEARNING_RATE, weight_decay=WEIGHT_DECAY)
        scheduler = optim.lr_scheduler.CosineAnnealingLR(optimizer, T_max=MAX_EPOCHS)

        n_params = sum(p.numel() for p in model.parameters() if p.requires_grad)
        if fold_idx == 0:
            print(f"  Model parameters: {n_params:,}")

        # Training loop
        best_auc = 0
        patience_counter = 0
        best_state = None

        for epoch in range(MAX_EPOCHS):
            train_loss = train_one_epoch(
                model, train_loader, optimizer, edge_index, edge_attr, device)
            scheduler.step()

            test_auc, _, _ = evaluate(model, test_loader, edge_index, edge_attr, device)

            if (epoch + 1) % 5 == 0:
                print(f"  Epoch {epoch+1}: loss={train_loss:.4f} "
                      f"test_AUC={test_auc:.4f} lr={scheduler.get_last_lr()[0]:.6f}")

            if test_auc > best_auc:
                best_auc = test_auc
                patience_counter = 0
                best_state = {k: v.cpu().clone() for k, v in model.state_dict().items()}
            else:
                patience_counter += 1
                if patience_counter >= PATIENCE:
                    print(f"  Early stopping at epoch {epoch+1}")
                    break

        # Restore best model
        if best_state:
            model.load_state_dict(best_state)
            model.to(device)

        # Final evaluation
        test_auc, test_probs, test_labels = evaluate(
            model, test_loader, edge_index, edge_attr, device)
        print(f"  Fold {fold_idx} best AUC: {test_auc:.4f}")

        fold_results.append({
            "fold": fold_idx,
            "train_days": f"{train_start:.0f}-{train_end:.0f}",
            "test_days": f"{t_start:.0f}-{t_end:.0f}",
            "auc_roc": round(test_auc, 4),
            "train_size": len(train_dataset),
            "test_size": len(test_dataset),
            "best_epoch": epoch + 1 - patience_counter,
        })

        all_test_probs.extend(test_probs)
        all_test_labels.extend(test_labels)

        # Checkpoint
        os.makedirs(CHECKPOINT_DIR, exist_ok=True)
        checkpoint_path = os.path.join(CHECKPOINT_DIR, f"gnn_fold_{fold_idx}.pt")
        torch.save({
            "model_state": best_state,
            "fold_idx": fold_idx,
            "auc": test_auc,
            "n_params": n_params,
        }, checkpoint_path)
        print(f"  Checkpoint saved: {checkpoint_path}")

    # Aggregate
    if fold_results:
        aucs = [f["auc_roc"] for f in fold_results]
        mean_auc = sum(aucs) / len(aucs)
        std_auc = (sum((a - mean_auc) ** 2 for a in aucs) / len(aucs)) ** 0.5
    else:
        mean_auc, std_auc = 0, 0

    pooled_auc = compute_auc(all_test_labels, all_test_probs) if all_test_labels else 0

    return {
        "n_folds": len(fold_results),
        "mean_auc": round(mean_auc, 4),
        "std_auc": round(std_auc, 4),
        "pooled_auc": round(pooled_auc, 4),
        "folds": fold_results,
    }


# ---------------------------------------------------------------------------
# Data Loading
# ---------------------------------------------------------------------------

def load_feature_matrix(path):
    """Load feature matrix from JSON export (same format as ConvLSTM)."""
    print(f"Loading feature matrix from {path}...")
    with open(path) as f:
        data = json.load(f)

    metadata = data["metadata"]
    n_timesteps = metadata["n_timesteps"]
    H = metadata["grid_h"]
    W = metadata["grid_w"]
    C = metadata["n_features"]

    print(f"  Shape: ({n_timesteps}, {H}, {W}, {C})")

    features = np.zeros((n_timesteps, H, W, C), dtype=np.float32)
    labels = np.zeros((n_timesteps, H, W), dtype=np.float32)
    times = np.zeros(n_timesteps, dtype=np.float64)

    for t_idx, ts in enumerate(data["timesteps"]):
        times[t_idx] = ts["t_days"]
        for i in range(H):
            for j in range(W):
                features[t_idx, i, j, :] = ts["features"][i][j]
                labels[t_idx, i, j] = ts["labels"][i][j]

    # Per-feature z-score normalization
    for c in range(C):
        vals = features[:, :, :, c]
        mean = vals.mean()
        std = vals.std()
        if std > 1e-6:
            features[:, :, :, c] = (vals - mean) / std

    n_pos = labels.sum()
    print(f"  Positive cells: {n_pos:.0f} ({100 * n_pos / (n_timesteps * H * W):.3f}%)")

    return features, labels, times, metadata


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    if not HAS_TORCH or not HAS_NUMPY:
        print("ERROR: PyTorch and NumPy required.")
        return
    if not HAS_PYG:
        print("ERROR: PyTorch Geometric required.")
        print("Install: pip install torch-geometric")
        return

    print("=" * 60)
    print("SeismoGNN: Graph Neural Network Earthquake Prediction")
    print("=" * 60)

    if torch.cuda.is_available():
        print(f"GPU: {torch.cuda.get_device_name(0)}")
        print(f"Memory: {torch.cuda.get_device_properties(0).total_mem / 1e9:.1f} GB")
    else:
        print("WARNING: No GPU detected.")

    # Build graph
    print("\nBuilding seismic graph...")
    edge_index, edge_attr, node_coords, node_zones = build_graph()

    # Load data
    if os.path.exists(FEATURE_MATRIX_PATH):
        matrix_path = FEATURE_MATRIX_PATH
    elif os.path.exists("results/feature_matrix.json"):
        matrix_path = "results/feature_matrix.json"
    else:
        print(f"ERROR: Feature matrix not found at {FEATURE_MATRIX_PATH}")
        return

    features, labels, times, metadata = load_feature_matrix(matrix_path)

    # Walk-forward CV
    print(f"\n{'=' * 60}")
    print("Walk-Forward Cross-Validation")
    print("=" * 60)

    start_time = time.time()
    cv_results = walk_forward_cv(
        features, labels, times,
        n_features=metadata["n_features"],
        edge_index=edge_index,
        edge_attr=edge_attr,
    )
    elapsed = time.time() - start_time

    print(f"\n{'=' * 60}")
    print(f"Results: mean_AUC={cv_results['mean_auc']:.4f} "
          f"(±{cv_results['std_auc']:.4f}) "
          f"pooled_AUC={cv_results['pooled_auc']:.4f}")
    print(f"Training time: {elapsed / 60:.1f} minutes")

    # Save results
    results = {
        "model": "SeismoGNN",
        "architecture": {
            "temporal_encoder": "GRU(2-layer)",
            "graph_layers": f"GATv2Conv × {GNN_LAYERS} (heads={GNN_HEADS})",
            "hidden_channels": GNN_HIDDEN,
            "edge_types": "8-neighbor + same-zone",
            "n_nodes": GRID_H * GRID_W,
            "n_edges": edge_index.shape[1],
            "pos_weight": POS_WEIGHT,
            "dropout": DROPOUT,
        },
        "training": {
            "lr": LEARNING_RATE,
            "weight_decay": WEIGHT_DECAY,
            "batch_size": BATCH_SIZE,
            "max_epochs": MAX_EPOCHS,
            "patience": PATIENCE,
            "scheduler": "CosineAnnealingLR",
            "grad_clip": 1.0,
            "device": str(torch.device("cuda" if torch.cuda.is_available() else "cpu")),
            "training_time_minutes": round(elapsed / 60, 1),
        },
        "graph": {
            "n_nodes": GRID_H * GRID_W,
            "n_edges": edge_index.shape[1],
            "avg_degree": round(edge_index.shape[1] / (GRID_H * GRID_W), 1),
            "zone_distribution": {},
        },
        "walk_forward_cv": cv_results,
        "metadata": metadata,
    }

    # Zone counts
    zone_counts = {}
    for z in node_zones:
        zone_counts[z] = zone_counts.get(z, 0) + 1
    results["graph"]["zone_distribution"] = zone_counts

    os.makedirs(os.path.dirname(RESULTS_PATH), exist_ok=True)
    with open(RESULTS_PATH, "w") as f:
        json.dump(results, f, indent=2, ensure_ascii=False)
    print(f"\nResults saved to {RESULTS_PATH}")


if __name__ == "__main__":
    main()
