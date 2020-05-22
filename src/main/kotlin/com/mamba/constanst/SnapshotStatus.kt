package com.mamba.constanst

enum class SnapshotStatus {
    /// Represents that the snapshot is finished being created.
    Finish,
    /// Indicates that the snapshot failed to build or is not ready.
    Failure,
}