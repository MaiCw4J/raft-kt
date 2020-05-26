package com.mamba

import com.google.protobuf.ByteString
import eraftpb.Eraftpb
import mu.KLogger
import java.util.*


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

data class ReadIndexStatus(val req: Eraftpb.Message.Builder, val index: Long, val acks: MutableSet<Long>)

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
    fun addRequest(index: Long, id: Long, m: Eraftpb.Message.Builder) {
        val ctx = m.entriesList.first().data
        if (ctx == null || ctx.isEmpty) {
            return
        }
        if (pendingReadIndex.containsKey(ctx)) {
            return
        }
        this.pendingReadIndex[ctx] = ReadIndexStatus(
            req = m,
            index = index,
            acks = hashSetOf(id)
        )
        this.readIndexQueue.push(ctx)
    }

    /// Returns the context of the last pending read only request in ReadOnly struct.
    fun lastPendingRequestCtx(): ByteString? = this.readIndexQueue.last

    /// Notifies the ReadOnly struct that the raft state machine received
    /// an acknowledgment of the heartbeat that attached with the read only request
    /// context.
    fun recvAck(id: Long, ctx: ByteString): Set<Long>? {
        return this.pendingReadIndex[ctx]?.let {
            it.acks.add(id)
            it.acks
        }
    }

    /// Advances the read only request queue kept by the ReadOnly struct.
    /// It de queues the requests until it finds the read only request that has
    /// the same context as the given `m`.
    fun advance(ctx: ByteString, logger: KLogger): Vec<ReadIndexStatus> {
        var index: Int? = null
        this.readIndexQueue.forEachIndexed { i, x ->
            if (!this.pendingReadIndex.containsKey(x)) {
                fatal(logger, "cannot find correspond read state from pending map")
            }
            if (x == ctx) {
                index = i
                return@forEachIndexed
            }
        }

        // not found readIndex request
        if (index == null) {
            return vec()
        }

        val rss = vec<ReadIndexStatus>()
        for (i in 0..index!!) {
            val rs = this.readIndexQueue.pollFirst()
            val status = this.pendingReadIndex.remove(rs)!!
            rss.add(status)
        }
        return rss
    }

}
