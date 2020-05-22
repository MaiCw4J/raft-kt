package com.mamba.storage

import com.mamba.RaftState
import com.mamba.Vec
import com.mamba.vec
import eraftpb.Eraftpb

class MemStorage: Storage {
    val raftState: RaftState
    // entries[i] has raft log position i+snapshot.get_metadata().index
    val entries: Vec<Eraftpb.Entry>
    // Metadata of the last snapshot received.
    val snapshotMetadata: Eraftpb.SnapshotMetadata

    constructor() {
        this.raftState = RaftState(Eraftpb.HardState.getDefaultInstance(), Eraftpb.ConfState.getDefaultInstance())
        this.entries = vec()
        this.snapshotMetadata = Eraftpb.SnapshotMetadata.getDefaultInstance()
    }

    override fun initialState(): RaftState {
        TODO("Not yet implemented")
    }

    override fun entries(low: Long, high: Long, maxSize: Long?): List<Eraftpb.Entry> {
        TODO("Not yet implemented")
    }

    override fun term(idx: Long): Long {
        TODO("Not yet implemented")
    }

    override fun firstIndex(): Long {
        TODO("Not yet implemented")
    }

    override fun lastIndex(): Long {
        TODO("Not yet implemented")
    }

    override fun snapshot(requestIndex: Long): Eraftpb.Snapshot {
        TODO("Not yet implemented")
    }
}