package com.mamba

import com.mamba.constanst.StateRole
import com.mamba.tracker.ProgressTracker
import eraftpb.Eraftpb

/// Represents the current status of the raft
class Status {
    /// The ID of the current node.
    val id: Long

    /// The hardstate of the raft, representing voted state.
    val hs: Eraftpb.HardState

    /// The softstate of the raft, representing proposed state.
    val ss: SoftState

    /// The index of the last entry to have been applied.
    val applied: Long

    /// The tracker towards catching up and applying logs.
    val progress: ProgressTracker?

    /// Gets a copy of the current raft status.
    constructor(raft: Raft<*>) {
        this.id = raft.id
        this.hs = raft.hardState()
        this.ss = raft.softState()
        this.applied = raft.raftLog.applied
        this.progress = if (this.ss.raftState == StateRole.Leader) raft.prs else null
    }

}