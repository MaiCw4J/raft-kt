package com.mamba

import com.google.protobuf.ByteString
import com.mamba.constanst.ProgressRole
import com.mamba.constanst.SnapshotStatus
import com.mamba.exception.RaftError
import com.mamba.exception.RaftErrorException
import com.mamba.storage.Storage
import eraftpb.Eraftpb

/// RawNode is a thread-unsafe Node.
/// The methods of this struct correspond to the methods of Node and are described
/// more fully there.
class RawNode<STORAGE : Storage> {
    /// The internal raft state.
    val raft: Raft<STORAGE>
    var ss: SoftState
    var hs: Eraftpb.HardState

    @Throws(RaftErrorException::class)
    constructor(config: Config, store: STORAGE) {
        this.raft = Raft(config, store)
        this.hs = this.raft.hardState()
        this.ss = this.raft.softState()
    }

    fun commitReady(ready: Ready) {
        ready.ss?.run {
            this@RawNode.ss = this
        }

        ready.hs?.run {
            if (this != Eraftpb.HardState.getDefaultInstance()) {
                this@RawNode.hs = this
            }
        }

        ready.entries.run {
            if (this.isNotEmpty()) {
                val last = this.last()
                this@RawNode.raft.raftLog.stableTo(last.index, last.term)
            }
        }

        ready.snapshot?.run {
            if (this != Eraftpb.Snapshot.getDefaultInstance()) {
                this@RawNode.raft.raftLog.stableSnapTo(this.metadata.index)
            }
        }

        if (ready.readStates.isNotEmpty()) {
            this.raft.readStates.clear()
        }
    }

    fun commitApply(applied: Long) = this.raft.commitApply(applied)

    /// Tick advances the internal logical clock by a single tick.
    ///
    /// Returns true to indicate that there will probably be some readiness which
    /// needs to be handled.
    fun tick(): Boolean = this.raft.tick()

    /// Campaign causes this RawNode to transition to candidate state.
    @Throws(RaftErrorException::class)
    fun campaign() = Eraftpb.Message.newBuilder().let {
        it.msgType = Eraftpb.MessageType.MsgHup
        this.raft.step(it)
    }

    /// Propose proposes data be appended to the raft log.
    @Throws(RaftErrorException::class)
    fun propose(context: ByteArray, data: ByteArray) {
        Eraftpb.Message.newBuilder().let {
            it.msgType = Eraftpb.MessageType.MsgPropose
            it.from = this.raft.id

            val entry = Eraftpb.Entry.newBuilder().apply {
                this.data = ByteString.copyFrom(data)
                this.context = ByteString.copyFrom(context)
            }
            it.addEntries(entry)

            this.raft.step(it)
        }
    }

    /// Broadcast heartbeats to all the followers.
    ///
    /// If it's not leader, nothing will happen.
    fun ping() = this.raft.ping()

    /// ProposeConfChange proposes a config change.
    @Throws(RaftErrorException::class)
    fun proposeConfChange(context: ByteArray, cc: Eraftpb.ConfChange) {
        Eraftpb.Message.newBuilder().let {
            it.msgType = Eraftpb.MessageType.MsgPropose

            val entry = Eraftpb.Entry.newBuilder().apply {
                this.data = cc.toByteString()
                this.context = ByteString.copyFrom(context)
            }
            it.addEntries(entry)

            this.raft.step(it)
        }
    }

    /// Takes the conf change and applies it.
    fun applyConfChange(cc: Eraftpb.ConfChange): Eraftpb.ConfState {
        if (cc.nodeId == INVALID_ID) {
            return Eraftpb.ConfState.newBuilder().apply {
                this.addAllVoters(this@RawNode.raft.prs.voterIds())
                this.addAllLearners(this@RawNode.raft.prs.learnerIds())
            }.build()
        }

        when (cc.changeType) {
            Eraftpb.ConfChangeType.AddNode -> this.raft.addVoterOrLearner(cc.nodeId, ProgressRole.VOTER)
            Eraftpb.ConfChangeType.AddLearnerNode -> this.raft.addVoterOrLearner(cc.nodeId, ProgressRole.LEARNER)
            Eraftpb.ConfChangeType.RemoveNode -> this.raft.removeNode(cc.nodeId)
            else -> { /* to do nothing */ }
        }

        return this.raft.prs.configuration.toConfState()
    }

    /// Step advances the state machine using the given message.
    @Throws(RaftErrorException::class)
    fun step(m: Eraftpb.Message) {
        // ignore unexpected local messages receiving over network
        if (m.msgType.isLocalMsg()) {
            raftError(RaftError.StepLocalMsg)
        }

        if (this.raft.prs.progress[m.from] != null || !m.msgType.isResponseMsg()) {
            this.raft.step(m.toBuilder())
        }

        raftError(RaftError.StepPeerNotFound)
    }

    /// Given an index, creates a new Ready value from that index.
    fun readySince(applied: Long): Ready =
        Ready(this.raft, this.ss, this.hs, applied)

    /// Ready returns the current point-in-time state of this RawNode.
    fun ready(): Ready =
        Ready(this.raft, this.ss, this.hs, null)

    /// Grabs the snapshot from the raft if available.
    fun snap(): Eraftpb.Snapshot? = this.raft.snap()

    /// Given an index, can determine if there is a ready state from that time.
    fun hasReadySince(applied: Long?): Boolean {
        // There are some messages that need to be sent to the peer
        if (this.raft.msgs.isNotEmpty()) {
            return true
        }

        // some log need to be persistent
        if (raft.raftLog.unstableEntries().let { it != null && it.isNotEmpty() }) {
            return true
        }

        // response readIndex request to client
        if (raft.readStates.isNotEmpty()) {
            return true
        }

        // snapshot is available
        if (this.snap()?.metadata?.index == 0L) {
            return true
        }

        // has commit log to the state machine
        val hasUnstableEntries = if (applied != null) {
            this.raft.raftLog.hasNextEntriesSince(applied)
        } else {
            this.raft.raftLog.hasNextEntries()
        }
        if (hasUnstableEntries) {
            return true
        }

        if (this.raft.softState() != this.ss) {
            return true
        }

        val rhs = raft.hardState()
        if (rhs != Eraftpb.HardState.getDefaultInstance() && rhs != this.hs) {
            return true
        }

        // nothing to do
        return false
    }

    /// HasReady called when RawNode user need to check if any Ready pending.
    /// Checking logic in this method should be consistent with Ready.containsUpdates().
    fun hasReady(): Boolean = this.hasReadySince(null)

    /// Appends and commits the ready value.
    fun advanceAppend(ready: Ready) = this.commitReady(ready)

    /// Advance apply to the passed index.
    fun advanceApply(applied: Long) = this.commitApply(applied)

    /// last Ready results.
    fun advance(ready: Ready) {
        this.advanceAppend(ready)

        val commitIdx = this.hs.commit
        if (commitIdx != 0L) {
            // In most cases, prevHardSt and rd.HardState will be the same
            // because when there are new entries to apply we just sent a
            // HardState with an updated Commit value. However, on initial
            // startup the two are different because we don't send a HardState
            // until something changes, but we do send any un-applied but
            // committed entries (and previously-committed entries may be
            // incorporated into the snapshot, even if rd.CommittedEntries is
            // empty). Therefore we mark all committed entries as applied
            // whether they were included in rd.HardState or not.
            this.advanceApply(commitIdx)
        }
    }

    /// Status returns the current status of the given group.
    fun status(): Status = Status(raft)

    /// ReportUnreachable reports the given node is not reachable for the last send.
    fun reportUnreachable(id: Long) {
        Eraftpb.Message.newBuilder().let {
            it.msgType = Eraftpb.MessageType.MsgUnreachable
            it.from = id
            // we don't care if it is ok actually
            this.raft.step(it)
        }
    }

    /// ReportSnapshot reports the status of the sent snapshot.
    fun reportSnapshot(id: Long, status: SnapshotStatus) {
        Eraftpb.Message.newBuilder().let {
            it.msgType = Eraftpb.MessageType.MsgSnapStatus
            it.from = id
            it.reject = status == SnapshotStatus.Failure
            // we don't care if it is ok actually
            this.raft.step(it)
        }
    }

    /// Request a snapshot from a leader.
    /// The snapshot's index must be greater or equal to the request_index.
    @Throws(RaftErrorException::class)
    fun requestSnapshot(requestIdx: Long) = this.raft.requestSnapshot(requestIdx)

    /// TransferLeader tries to transfer leadership to the given transferee.
    fun transferLeader(transferee: Long) {
        Eraftpb.Message.newBuilder().let {
            it.msgType = Eraftpb.MessageType.MsgTransferLeader
            it.from = transferee
            // we don't care if it is ok actually
            this.raft.step(it)
        }
    }

    /// ReadIndex requests a read state. The read state will be set in ready.
    /// Read State has a read index. Once the application advances further than the read
    /// index, any linearizable read requests issued before the read request can be
    /// processed safely. The read state will have the same ctx attached.
    fun readIndex(ctx: ByteArray) {
        Eraftpb.Message.newBuilder().let {
            it.msgType = Eraftpb.MessageType.MsgReadIndex
            val entry = Eraftpb.Entry.newBuilder().apply {
                this.data = ByteString.copyFrom(ctx)
            }
            it.addEntries(entry)
            // we don't care if it is ok actually
            this.raft.step(it)
        }
    }

    /// Returns the store as an immutable reference.
    fun store(): STORAGE = this.raft.raftLog.store

    /// Set whether skip broadcast empty commit messages at runtime.
    fun skipBcastCommit(skip: Boolean) = this.raft.skipBcastCommit(skip)

    /// Set whether to batch append msg at runtime.
    fun setBatchAppend(batchAppend: Boolean) = this.raft.batchAppend(batchAppend)

    /// Set priority of node.
    fun setPriority(priority: Long) {
        this.raft.priority = priority
    }

}