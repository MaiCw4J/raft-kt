package com.mamba.constanst

enum class ProgressState {
    /// Whether it's probing.
    Probe,

    /// Whether it's replicating.
    Replicate,

    /// Whether it's a snapshot.
    Snapshot,
}