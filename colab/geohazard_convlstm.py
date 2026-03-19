"""ConvLSTM spatiotemporal earthquake prediction — Colab GPU training.

Architecture:
    Input: (batch, T=30, H=11, W=11, C) — 30 steps × 3 days = 90 days history
    → Conv2D(C→64, 3×3, padding=1) — spatial feature extraction
    → ConvLSTM2d(64→64, 3×3, padding=1) × 2 layers
    → Conv2D(64→1, 1×1) — per-cell probability
    → Sigmoid
    Output: (batch, H=11, W=11)

    C is dynamic (read from feature_matrix.json metadata).
    Phase 13: up to 79 features; stability selection may reduce to ~64.

Loss: BCE with pos_weight=20
Optimizer: Adam, lr=1e-3, weight_decay=1e-5
Walk-Forward CV: same splits as HistGBT for fair comparison

Data flow:
    Google Drive ← RPi5 downloads from GitHub Actions artifacts
    → This script loads feature_matrix.json from Drive
    → Converts to PyTorch tensors
    → Trains ConvLSTM with walk-forward CV
    → Saves results to Drive

Usage (Colab):
    1. Mount Google Drive
    2. Run this script
    3. Results saved to Drive for comparison

References:
    - Shi et al. (2015) "Convolutional LSTM Network"
    - DeVries et al. (2018) "Deep learning of aftershock patterns"
    - Mignan & Broccardo (2020) "Neural network applications in earthquake prediction"
"""

import json
import math
import os
import sys
import time
from pathlib import Path

# Check for PyTorch
try:
    import torch
    import torch.nn as nn
    import torch.optim as optim
    from torch.utils.data import Dataset, DataLoader
    HAS_TORCH = True
except ImportError:
    HAS_TORCH = False
    print("PyTorch not available. Install with: pip install torch")

# Check for numpy
try:
    import numpy as np
    HAS_NUMPY = True
except ImportError:
    HAS_NUMPY = False


# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

SEQUENCE_LENGTH = 30  # 30 time steps × 3 days = 90 days of history
POS_WEIGHT = 20.0     # BCE positive class weight (extreme imbalance)
LEARNING_RATE = 1e-3
WEIGHT_DECAY = 1e-5
BATCH_SIZE = 32
MAX_EPOCHS = 50
PATIENCE = 10         # Early stopping patience

# Drive paths (Colab)
DRIVE_BASE = "/content/drive/MyDrive/kaggle/geohazard"
FEATURE_MATRIX_PATH = os.path.join(DRIVE_BASE, "feature_matrix.json")
RESULTS_PATH = os.path.join(DRIVE_BASE, "convlstm_results.json")
CHECKPOINT_DIR = os.path.join(DRIVE_BASE, "checkpoints")


# ---------------------------------------------------------------------------
# ConvLSTM Cell
# ---------------------------------------------------------------------------

class ConvLSTMCell(nn.Module):
    """Single ConvLSTM cell."""

    def __init__(self, input_channels, hidden_channels, kernel_size=3):
        super().__init__()
        self.hidden_channels = hidden_channels
        padding = kernel_size // 2

        self.conv = nn.Conv2d(
            input_channels + hidden_channels,
            4 * hidden_channels,  # i, f, g, o gates
            kernel_size=kernel_size,
            padding=padding,
            bias=True,
        )

    def forward(self, x, state):
        h, c = state
        combined = torch.cat([x, h], dim=1)
        gates = self.conv(combined)

        i, f, g, o = torch.split(gates, self.hidden_channels, dim=1)
        i = torch.sigmoid(i)
        f = torch.sigmoid(f)
        g = torch.tanh(g)
        o = torch.sigmoid(o)

        c_new = f * c + i * g
        h_new = o * torch.tanh(c_new)

        return h_new, c_new

    def init_state(self, batch_size, height, width, device):
        return (
            torch.zeros(batch_size, self.hidden_channels, height, width, device=device),
            torch.zeros(batch_size, self.hidden_channels, height, width, device=device),
        )


# ---------------------------------------------------------------------------
# ConvLSTM Model
# ---------------------------------------------------------------------------

class ConvLSTMPredictor(nn.Module):
    """ConvLSTM for spatiotemporal earthquake prediction."""

    def __init__(self, n_features=64, hidden_channels=64, n_layers=2):
        super().__init__()

        # Spatial feature extraction
        self.spatial_conv = nn.Sequential(
            nn.Conv2d(n_features, hidden_channels, 3, padding=1),
            nn.BatchNorm2d(hidden_channels),
            nn.ReLU(inplace=True),
        )

        # ConvLSTM layers
        self.convlstm_layers = nn.ModuleList()
        for i in range(n_layers):
            in_ch = hidden_channels
            self.convlstm_layers.append(
                ConvLSTMCell(in_ch, hidden_channels, kernel_size=3)
            )

        # Channel attention (squeeze-excitation)
        self.se = nn.Sequential(
            nn.AdaptiveAvgPool2d(1),
            nn.Flatten(),
            nn.Linear(hidden_channels, hidden_channels // 4),
            nn.ReLU(inplace=True),
            nn.Linear(hidden_channels // 4, hidden_channels),
            nn.Sigmoid(),
        )

        # Output: per-cell probability
        self.output_conv = nn.Conv2d(hidden_channels, 1, 1)

    def forward(self, x):
        """
        Args:
            x: (batch, T, H, W, C) — sequence of spatial feature grids

        Returns:
            (batch, H, W) — per-cell probability
        """
        batch, T, H, W, C = x.shape

        # Process each time step
        states = [
            layer.init_state(batch, H, W, x.device)
            for layer in self.convlstm_layers
        ]

        for t in range(T):
            # (batch, C, H, W) — channels first for conv
            xt = x[:, t].permute(0, 3, 1, 2)

            # Spatial feature extraction
            xt = self.spatial_conv(xt)

            # ConvLSTM layers
            for i, layer in enumerate(self.convlstm_layers):
                h, c = layer(xt, states[i])
                states[i] = (h, c)
                xt = h  # input to next layer

        # Final hidden state
        h_final = states[-1][0]  # (batch, hidden, H, W)

        # Channel attention
        se_weights = self.se(h_final)  # (batch, hidden)
        se_weights = se_weights.unsqueeze(-1).unsqueeze(-1)  # (batch, hidden, 1, 1)
        h_final = h_final * se_weights

        # Output
        out = self.output_conv(h_final)  # (batch, 1, H, W)
        out = torch.sigmoid(out)
        return out.squeeze(1)  # (batch, H, W)


# ---------------------------------------------------------------------------
# Dataset
# ---------------------------------------------------------------------------

class EarthquakeDataset(Dataset):
    """Spatiotemporal earthquake dataset for ConvLSTM."""

    def __init__(self, features, labels, times, seq_length=SEQUENCE_LENGTH):
        """
        Args:
            features: (n_timesteps, H, W, C) numpy array
            labels: (n_timesteps, H, W) numpy array
            times: (n_timesteps,) array of t_days
            seq_length: number of time steps per sequence
        """
        self.features = features
        self.labels = labels
        self.times = times
        self.seq_length = seq_length

        # Valid indices: need seq_length history + 1 for target
        self.valid_indices = list(range(seq_length, len(features)))

    def __len__(self):
        return len(self.valid_indices)

    def __getitem__(self, idx):
        t = self.valid_indices[idx]
        x = self.features[t - self.seq_length:t]  # (T, H, W, C)
        y = self.labels[t]  # (H, W) — predict next step
        return (
            torch.FloatTensor(x),
            torch.FloatTensor(y),
            self.times[t],
        )


# ---------------------------------------------------------------------------
# Training
# ---------------------------------------------------------------------------

def train_one_epoch(model, loader, optimizer, criterion, device):
    """Train for one epoch."""
    model.train()
    total_loss = 0
    n_batches = 0

    for x, y, _ in loader:
        x, y = x.to(device), y.to(device)

        optimizer.zero_grad()
        pred = model(x)
        loss = criterion(pred, y)
        loss.backward()
        optimizer.step()

        total_loss += loss.item()
        n_batches += 1

    return total_loss / max(n_batches, 1)


def evaluate(model, loader, device):
    """Evaluate model, return AUC and loss."""
    model.eval()
    all_probs = []
    all_labels = []

    with torch.no_grad():
        for x, y, _ in loader:
            x, y = x.to(device), y.to(device)
            pred = model(x)
            all_probs.extend(pred.cpu().numpy().flatten().tolist())
            all_labels.extend(y.cpu().numpy().flatten().tolist())

    # Compute AUC
    auc = compute_auc(all_labels, all_probs)
    return auc, all_probs, all_labels


def compute_auc(y_true, y_prob):
    """Compute AUC-ROC (pure Python)."""
    n_pos = sum(y_true)
    n_neg = len(y_true) - n_pos
    if n_pos == 0 or n_neg == 0:
        return 0.5

    combined = sorted(zip(y_prob, y_true), key=lambda x: -x[0])
    tp, fp = 0, 0
    prev_fpr = 0
    prev_tpr = 0
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

def walk_forward_cv(features, labels, times, n_features,
                    initial_train_years=5, step_years=1, test_years=1):
    """Walk-forward cross-validation for ConvLSTM.

    Same split logic as HistGBT for fair comparison.
    """
    device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
    print(f"Device: {device}")

    day_min = times[0]
    day_max = times[-1]
    initial_train_days = initial_train_years * 365.25
    step_days = step_years * 365.25
    test_days = test_years * 365.25

    # Generate splits
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

        # Split by time
        train_mask = (times >= train_start + SEQUENCE_LENGTH * 3) & (times < train_end)
        test_mask = (times >= t_start) & (times < t_end)

        train_indices = np.where(train_mask)[0]
        test_indices = np.where(test_mask)[0]

        if len(train_indices) < 100 or len(test_indices) < 50:
            print(f"  Skipping fold {fold_idx}: insufficient data")
            continue

        # Create datasets
        train_dataset = EarthquakeDataset(features, labels, times, SEQUENCE_LENGTH)
        test_dataset = EarthquakeDataset(features, labels, times, SEQUENCE_LENGTH)

        # Filter to time range
        train_dataset.valid_indices = [
            i for i in train_dataset.valid_indices
            if train_start <= times[i] < train_end
        ]
        test_dataset.valid_indices = [
            i for i in test_dataset.valid_indices
            if t_start <= times[i] < t_end
        ]

        n_pos_train = sum(
            labels[i].sum() for i in train_dataset.valid_indices
        )
        n_pos_test = sum(
            labels[i].sum() for i in test_dataset.valid_indices
        )
        print(f"  Train: {len(train_dataset)} samples (pos={n_pos_train:.0f})")
        print(f"  Test: {len(test_dataset)} samples (pos={n_pos_test:.0f})")

        if n_pos_train < 5:
            print(f"  Skipping fold {fold_idx}: too few positives")
            continue

        train_loader = DataLoader(train_dataset, batch_size=BATCH_SIZE, shuffle=True)
        test_loader = DataLoader(test_dataset, batch_size=BATCH_SIZE)

        # Model
        model = ConvLSTMPredictor(n_features=n_features).to(device)
        optimizer = optim.Adam(model.parameters(), lr=LEARNING_RATE, weight_decay=WEIGHT_DECAY)
        criterion = nn.BCELoss(
            weight=None,
            reduction='none',
        )

        def weighted_bce(pred, target):
            """BCE with pos_weight for imbalanced data."""
            weight = torch.where(target >= 0.5, POS_WEIGHT, 1.0)
            loss = nn.functional.binary_cross_entropy(pred, target, reduction='none')
            return (loss * weight).mean()

        # Training loop
        best_auc = 0
        patience_counter = 0
        best_state = None

        for epoch in range(MAX_EPOCHS):
            train_loss = train_one_epoch(model, train_loader, optimizer, weighted_bce, device)
            test_auc, _, _ = evaluate(model, test_loader, device)

            if (epoch + 1) % 5 == 0:
                print(f"  Epoch {epoch+1}: loss={train_loss:.4f} test_AUC={test_auc:.4f}")

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
        test_auc, test_probs, test_labels = evaluate(model, test_loader, device)
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

        # Save checkpoint per fold (12h Colab limit protection)
        os.makedirs(CHECKPOINT_DIR, exist_ok=True)
        checkpoint_path = os.path.join(CHECKPOINT_DIR, f"fold_{fold_idx}.pt")
        torch.save({
            "model_state": best_state,
            "fold_idx": fold_idx,
            "auc": test_auc,
        }, checkpoint_path)
        print(f"  Checkpoint saved: {checkpoint_path}")

    # Aggregate
    if fold_results:
        mean_auc = sum(f["auc_roc"] for f in fold_results) / len(fold_results)
        aucs = [f["auc_roc"] for f in fold_results]
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
# Main
# ---------------------------------------------------------------------------

def load_feature_matrix(path):
    """Load feature matrix from JSON export."""
    print(f"Loading feature matrix from {path}...")
    with open(path) as f:
        data = json.load(f)

    metadata = data["metadata"]
    n_timesteps = metadata["n_timesteps"]
    H = metadata["grid_h"]
    W = metadata["grid_w"]
    C = metadata["n_features"]

    print(f"  Shape: ({n_timesteps}, {H}, {W}, {C})")

    # Convert to numpy arrays
    features = np.zeros((n_timesteps, H, W, C), dtype=np.float32)
    labels = np.zeros((n_timesteps, H, W), dtype=np.float32)
    times = np.zeros(n_timesteps, dtype=np.float64)

    for t_idx, ts in enumerate(data["timesteps"]):
        times[t_idx] = ts["t_days"]
        for i in range(H):
            for j in range(W):
                features[t_idx, i, j, :] = ts["features"][i][j]
                labels[t_idx, i, j] = ts["labels"][i][j]

    # Normalize features (per-feature z-score)
    for c in range(C):
        vals = features[:, :, :, c]
        mean = vals.mean()
        std = vals.std()
        if std > 1e-6:
            features[:, :, :, c] = (vals - mean) / std

    n_pos = labels.sum()
    print(f"  Positive cells: {n_pos:.0f} ({100*n_pos/(n_timesteps*H*W):.3f}%)")

    return features, labels, times, metadata


def main():
    if not HAS_TORCH or not HAS_NUMPY:
        print("ERROR: PyTorch and NumPy required. Run on Colab with GPU runtime.")
        return

    print("=" * 60)
    print("ConvLSTM Spatiotemporal Earthquake Prediction")
    print("=" * 60)

    # Check for GPU
    if torch.cuda.is_available():
        print(f"GPU: {torch.cuda.get_device_name(0)}")
        print(f"Memory: {torch.cuda.get_device_properties(0).total_mem / 1e9:.1f} GB")
    else:
        print("WARNING: No GPU detected. Training will be slow.")

    # Load data
    if os.path.exists(FEATURE_MATRIX_PATH):
        matrix_path = FEATURE_MATRIX_PATH
    elif os.path.exists("results/feature_matrix.json"):
        matrix_path = "results/feature_matrix.json"
    else:
        print(f"ERROR: Feature matrix not found at {FEATURE_MATRIX_PATH}")
        print("Run export_feature_matrix.py first, then sync to Drive.")
        return

    features, labels, times, metadata = load_feature_matrix(matrix_path)

    # Walk-forward CV
    print("\n" + "=" * 60)
    print("Walk-Forward Cross-Validation")
    print("=" * 60)

    start_time = time.time()
    cv_results = walk_forward_cv(
        features, labels, times,
        n_features=metadata["n_features"],
    )
    elapsed = time.time() - start_time

    print(f"\n{'='*60}")
    print(f"Results: mean_AUC={cv_results['mean_auc']:.4f} "
          f"(±{cv_results['std_auc']:.4f}) "
          f"pooled_AUC={cv_results['pooled_auc']:.4f}")
    print(f"Training time: {elapsed/60:.1f} minutes")

    # Save results
    results = {
        "model": "ConvLSTM",
        "architecture": {
            "input_shape": f"(batch, {SEQUENCE_LENGTH}, {metadata['grid_h']}, "
                          f"{metadata['grid_w']}, {metadata['n_features']})",
            "hidden_channels": 64,
            "n_layers": 2,
            "channel_attention": True,
            "pos_weight": POS_WEIGHT,
        },
        "training": {
            "lr": LEARNING_RATE,
            "weight_decay": WEIGHT_DECAY,
            "batch_size": BATCH_SIZE,
            "max_epochs": MAX_EPOCHS,
            "patience": PATIENCE,
            "device": str(torch.device("cuda" if torch.cuda.is_available() else "cpu")),
            "training_time_minutes": round(elapsed / 60, 1),
        },
        "walk_forward_cv": cv_results,
        "metadata": metadata,
    }

    os.makedirs(os.path.dirname(RESULTS_PATH), exist_ok=True)
    with open(RESULTS_PATH, "w") as f:
        json.dump(results, f, indent=2, ensure_ascii=False)
    print(f"\nResults saved to {RESULTS_PATH}")


if __name__ == "__main__":
    main()
