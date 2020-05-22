package com.mamba.storage

import com.mamba.RaftState
import com.mamba.exception.RaftErrorException
import eraftpb.Eraftpb

/// Storage saves all the information about the current Raft implementation, including Raft Log,
/// commit index, the leader to vote for, etc.
///
/// If any Storage method returns an error, the raft instance will
/// become inoperable and refuse to participate in elections; the
/// application is responsible for cleanup and recovery in this case.
interface Storage {
    /// `initial_state` is called when Raft is initialized. This interface will return a `RaftState`
    /// which contains `HardState` and `ConfState`.
    ///
    /// `RaftState` could be initialized or not. If it's initialized it means the `Storage` is
    /// created with a configuration, and its last index and term should be greater than 0.
    @Throws(RaftErrorException::class)
    fun initialState(): RaftState

    /// Returns a slice of log entries in the range `[low, high)`.
    /// max_size limits the total size of the log entries returned if not `None`, however
    /// the slice of entries returned will always have length at least 1 if entries are
    /// found in the range.
    ///
    /// # Panics
    ///
    /// Panics if `high` is higher than `Storage::last_index(&self) + 1`.
    @Throws(RaftErrorException::class)
    fun entries(low: Long, high: Long, maxSize: Long?): List<Eraftpb.Entry>

    /// Returns the term of entry idx, which must be in the range
    /// [first_index()-1, last_index()]. The term of the entry before
    /// first_index is retained for matching purpose even though the
    /// rest of that entry may not be available.
    @Throws(RaftErrorException::class)
    fun term(idx: Long): Long

    /// Returns the index of the first log entry that is possible available via entries, which will
    /// always equal to `truncated index` plus 1.
    ///
    /// New created (but not initialized) `Storage` can be considered as truncated at 0 so that 1
    /// will be returned in this case.
    @Throws(RaftErrorException::class)
    fun firstIndex(): Long

    /// The index of the last entry replicated in the `Storage`.
    @Throws(RaftErrorException::class)
    fun lastIndex(): Long

    /// Returns the most recent snapshot.
    ///
    /// If snapshot is temporarily unavailable, it should return SnapshotTemporarilyUnavailable,
    /// so raft state machine could know that Storage needs some time to prepare
    /// snapshot and call snapshot later.
    /// A snapshot's index must not less than the `request_index`.
    @Throws(RaftErrorException::class)
    fun snapshot(requestIndex: Long): Eraftpb.Snapshot
}