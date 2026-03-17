"""Multi-target prediction configuration for M5+, M5.5+, M6+.

Each target defines the minimum magnitude, prediction window, time step,
minimum positive samples for training, and class weight ratio for
handling extreme class imbalance (M6+).
"""

TARGET_CONFIGS = {
    "M5+": {
        "min_mag": 5.0,
        "window_days": 7,
        "step_days": 3,
        "min_pos_train": 10,
        "min_pos_test": 5,
        "class_weight_ratio": 1,
    },
    "M5.5+": {
        "min_mag": 5.5,
        "window_days": 7,
        "step_days": 3,
        "min_pos_train": 8,
        "min_pos_test": 3,
        "class_weight_ratio": 3,
    },
    "M6+": {
        "min_mag": 6.0,
        "window_days": 14,
        "step_days": 3,
        "min_pos_train": 5,
        "min_pos_test": 2,
        "class_weight_ratio": 10,
    },
}

DEFAULT_TARGET = "M5+"
