package com.mamba.progress

import com.mamba.constanst.ProgressState
import com.mamba.INVALID_INDEX
import com.mamba.exception.PanicException
import kotlin.math.max
import kotlin.math.min

class Progress {
    /// How much state is matched.
    var matched: Long
    /// The next index to apply
    var nextIdx: Long
    /// When in ProgressStateProbe, leader sends at most one replication message
    /// per heartbeat interval. It also probes actual progress of the follower.
    ///
    /// When in ProgressStateReplicate, leader optimistically increases next
    /// to the latest entry sent after sending replication message. This is
    /// an optimized state for fast replicating log entries to the follower.
    ///
    /// When in ProgressStateSnapshot, leader should have sent out snapshot
    /// before and stop sending any replication message.
    var state: ProgressState
    /// Paused is used in ProgressStateProbe.
    /// When Paused is true, raft should pause sending replication message to this peer.
    var paused: Boolean
    /// This field is used in ProgressStateSnapshot.
    /// If there is a pending snapshot, the pendingSnapshot will be set to the
    /// index of the snapshot. If pendingSnapshot is set, the replication process of
    /// this Progress will be paused. raft will not resend snapshot until the pending one
    /// is reported to be failed.
    var pendingSnapshot: Long
    /// This field is used in request snapshot.
    /// If there is a pending request snapshot, this will be set to the request
    /// index of the snapshot.
    var pendingRequestSnapshot: Long

    /// This is true if the progress is recently active. Receiving any messages
    /// from the corresponding follower indicates the progress is active.
    /// RecentActive can be reset to false after an election timeout.
    var recentActive: Boolean

    /// Inflights is a sliding window for the inflight messages.
    /// When inflights is full, no more message should be sent.
    /// When a leader sends out a message, the index of the last
    /// entry should be added to inflights. The index MUST be added
    /// into inflights in order.
    /// When a leader receives a reply, the previous inflights should
    /// be freed by calling inflights.freeTo.
    val ins: Inflights

    constructor(nextIdx: Long, insSize: Int) {
        this.matched = 0
        this.nextIdx = nextIdx
        this.state = ProgressState.Probe
        this.paused = false
        this.pendingSnapshot = 0
        this.pendingRequestSnapshot = 0
        this.recentActive = false
        this.ins = Inflights(insSize)
    }

    private fun resetState(state: ProgressState) {
        this.paused = false
        this.pendingSnapshot = 0
        this.state = state
        this.ins.reset()
    }

    fun reset(nextIdx: Long) {
        this.matched = 0
        this.nextIdx = nextIdx
        this.state = ProgressState.Probe
        this.paused = false
        this.pendingSnapshot = 0
        this.pendingRequestSnapshot = 0
        this.recentActive = false
        this.ins.reset()
    }

    /// Changes the progress to a probe.
    fun becomeProbe() {
        // If the original state is ProgressStateSnapshot, progress knows that
        // the pending snapshot has been sent to this peer successfully, then
        // probes from pendingSnapshot + 1.
        if (this.state == ProgressState.Snapshot) {
            val prePendingSnapshot = this.pendingRequestSnapshot;
            this.resetState(ProgressState.Probe)
            this.nextIdx = max(this.matched, prePendingSnapshot) + 1
        } else {
            this.resetState(ProgressState.Probe)
            this.nextIdx = this.matched + 1
        }
    }

    /// Changes the progress to a Replicate.
    fun becomeReplicate() {
        this.resetState(ProgressState.Replicate)
        this.nextIdx = this.matched + 1
    }

    /// Changes the progress to a snapshot.
    fun becomeSnapshot(snapshotIdx: Long) {
        this.resetState(ProgressState.Snapshot)
        this.pendingSnapshot = snapshotIdx
    }

    /// Sets the snapshot to failure.
    fun snapshotFailure() {
        this.pendingSnapshot = 0
    }

    /// Unsets pendingSnapshot if Match is equal or higher than
    /// the pendingSnapshot
    fun maybeSnapshotAbort(): Boolean = this.state == ProgressState.Snapshot && this.matched >= this.pendingSnapshot

    /// Returns false if the given n index comes from an outdated message.
    /// Otherwise it updates the progress and returns true.
    fun maybeUpdate(n: Long): Boolean {
        val needUpdate = this.matched < n
        if (needUpdate) {
            this.matched = n
            this.resume()
        }

        if (this.nextIdx < n + 1) {
            this.nextIdx = n + 1
        }
        return needUpdate
    }

    /// Resume progress
    fun resume() {
        this.paused = false
    }

    /// Pause progress.
    fun pause() {
        this.paused = true
    }

    /// Optimistically advance the index
    private fun optimisticUpdate(n: Long) {
        this.nextIdx = n + 1
    }

    /// Returns false if the given index comes from an out of order message.
    /// Otherwise it decreases the progress next index to min(rejected, last)
    /// and returns true.
    fun maybeDecrTo(rejected: Long, last: Long, requestSnapshot: Long): Boolean {
        if (this.state == ProgressState.Replicate) {
            // the rejection must be stale if the progress has matched and "rejected"
            // is smaller than "match".
            // Or rejected equals to matched and request_snapshot is the INVALID_INDEX.
            if (rejected < this.matched || (rejected == this.matched && requestSnapshot == INVALID_INDEX)) {
                return false
            }

            if (requestSnapshot == INVALID_INDEX) {
                this.nextIdx = this.matched + 1
            } else {
                this.pendingRequestSnapshot = requestSnapshot
            }

            return true
        }

        // The rejection must be stale if "rejected" does not match next - 1.
        // Do not consider it stale if it is a request snapshot message.
        if (this.nextIdx == INVALID_INDEX || (this.nextIdx - 1) != rejected) {
            return false
        }

        // Do not decrease next index if it's requesting snapshot.
        if (requestSnapshot == INVALID_INDEX) {
            val min = min(rejected, last + 1)
            // always greater than zero
            this.nextIdx = if (min  < 1) 1 else min
        } else {
            // Allow requesting snapshot even if it's not Replicate.
            this.pendingRequestSnapshot = requestSnapshot
        }
        this.resume()
        return true
    }

    /// Determine whether progress is paused.
    fun isPaused(): Boolean = when(this.state) {
        ProgressState.Probe -> this.paused
        ProgressState.Replicate -> this.ins.full()
        ProgressState.Snapshot -> true
    }

    /// Update inflight msgs and next_idx
    fun updateState(last: Long) {
        when(this.state) {
            ProgressState.Replicate -> {
                this.optimisticUpdate(last)
                this.ins.add(last)
            }
            ProgressState.Probe -> this.pause()
            ProgressState.Snapshot -> throw PanicException("updating progress state in unhandled state ${this.state}")
        }
    }

}