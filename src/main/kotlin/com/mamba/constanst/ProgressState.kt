package com.mamba.constanst

/**
 *
 * @author mcw
 * @date 2020/04/09 10:18
 *
 */
enum class ProgressState {
    /// Whether it's probing.
    Probe,
    /// Whether it's replicating.
    Replicate,
    /// Whether it's a snapshot.
    Snapshot,
}