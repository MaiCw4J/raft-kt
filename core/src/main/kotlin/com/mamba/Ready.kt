package com.mamba

import eraftpb.Eraftpb

/// Ready encapsulates the entries and messages that are ready to read,
/// be saved to stable storage, committed or sent to other peers.
/// All fields in Ready are read-only.
class Ready {
    val ss: SoftState?

    val hs: Eraftpb.HardState?

    val readStates: List<ReadState>

    val entries: List<Eraftpb.Entry>

    val snapshot: Eraftpb.Snapshot?

    /// CommittedEntries specifies entries to be committed to a
    /// store/state-machine. These have previously been committed to stable
    /// store.
    val committedEntries: List<Eraftpb.Entry>

    /// Messages specifies outbound messages to be sent AFTER Entries are
    /// committed to stable storage.
    /// If it contains a MsgSnap message, the application MUST report back to raft
    /// when the snapshot has been received or has failed by calling ReportSnapshot.
    val messages: List<Eraftpb.Message>

    val mustSync: Boolean

    constructor(
        raft: Raft<*>,
        ss: SoftState,
        hs: Eraftpb.HardState,
        sinceIdx: Long?
    ) {

        this.entries = raft.raftLog.unstableEntries()?.map { it.build() } ?: listOf()

        val transport = raft.msgs
        if (transport.isEmpty()) {
            this.messages = listOf()
        } else {
            this.messages = transport.map { it.build() }
            // clear already send message
            transport.clear()
        }

        this.committedEntries = if (sinceIdx == null) {
            raft.raftLog.nextEntries()
        } else {
            raft.raftLog.nextEntriesSince(sinceIdx)
        } ?: listOf()

        this.ss = if (raft.softState() != ss) ss else null

        val rhs = raft.hardState()
        if (rhs != hs) {
            this.mustSync = rhs.vote != hs.vote || rhs.term != hs.term || this.entries.isNotEmpty()
            this.hs = rhs
        } else {
            // default
            this.mustSync = false
            this.hs = null
        }

        this.snapshot = raft.raftLog.unstable.snapshot

        // todo return immutable list
        this.readStates = raft.readStates
    }
}
