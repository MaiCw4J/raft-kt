package com.mamba

import com.google.protobuf.ByteString
import eraftpb.Eraftpb
import java.util.ArrayDeque

enum class ReadOnlyOption {
    /// Safe guarantees the linearizability of the read only request by
    /// communicating with the quorum. It is the default and suggested option.
    Safe,
    /// LeaseBased ensures linearizability of the read only request by
    /// relying on the leader lease. It can be affected by clock drift.
    /// If the clock drift is unbounded, leader might keep the lease longer than it
    /// should (clock can move backward/pause without any bound). ReadIndex is not safe
    /// in that case.
    LeaseBased,
}

data class ReadIndexStatus(val req: Eraftpb.Message.Builder, val index: Long, val acks: Set<Long>)

data class ReadState(val index: Long, val requestCtx: ByteString)

class ReadOnly {
    val option: ReadOnlyOption
    private val pendingReadIndex: MutableMap<ByteString, ReadIndexStatus>
    private val readIndexQueue: ArrayDeque<ByteString>

    constructor(option: ReadOnlyOption) {
        this.option = option
        this.pendingReadIndex = hashMapOf()
        this.readIndexQueue = ArrayDeque()
    }

    /// Adds a read only request into readonly struct.
    ///
    /// `index` is the commit index of the raft state machine when it received
    /// the read only request.
    ///
    /// `m` is the original read only request message from the local or remote node.
    fun addRequest(index: Long, m: Eraftpb.Message.Builder) {
        val ctx = m.entriesList.first().data
        if (pendingReadIndex.containsKey(ctx)) {
            return
        }
        this.pendingReadIndex[ctx] = ReadIndexStatus(
            req = m,
            index = index,
            acks = hashSetOf()
        )
        this.readIndexQueue.push(ctx)
    }

    /// Returns the context of the last pending read only request in ReadOnly struct.
    fun lastPendingRequestCtx(): ByteString? = this.readIndexQueue.last

}
