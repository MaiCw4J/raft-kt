package com.mamba

import com.mamba.exception.RaftError
import com.mamba.exception.RaftErrorException

/// Config contains the parameters to start a raft.
class Config {

    /// The identity of the local raft. It cannot be 0, and must be unique in the group.
    val id: Long

    /// The number of node.tick invocations that must pass between
    /// elections. That is, if a follower does not receive any message from the
    /// leader of current term before ElectionTick has elapsed, it will become
    /// candidate and start an election. election_tick must be greater than
    /// HeartbeatTick. We suggest election_tick = 10 * HeartbeatTick to avoid
    /// unnecessary leader switching
    val electionTick: Int

    /// HeartbeatTick is the number of node.tick invocations that must pass between
    /// heartbeats. That is, a leader sends heartbeat messages to maintain its
    /// leadership every heartbeat ticks.
    val heartbeatTick: Int

    /// Applied is the last applied index. It should only be set when restarting
    /// raft. raft will not return entries to the application smaller or equal to Applied.
    /// If Applied is unset when restarting, raft might return previous applied entries.
    /// This is a very application dependent configuration.
    val applied: Long

    /// Limit the max size of each append message. Smaller value lowers
    /// the raft recovery cost(initial probing and message lost during normal operation).
    /// On the other side, it might affect the throughput during normal replication.
    /// Note: math.MaxUusize64 for unlimited, 0 for at most one entry per message.
    val maxSizePerMsg: Long

    /// Limit the max number of in-flight append messages during optimistic
    /// replication phase. The application transportation layer usually has its own sending
    /// buffer over TCP/UDP. Set to avoid overflowing that sending buffer.
    /// TODO: feedback to application to limit the proposal rate?
    val maxInflightMsgs: Int

    /// Specify if the leader should check quorum activity. Leader steps down when
    /// quorum is not active for an electionTimeout.
    val checkQuorum: Boolean

    /// Enables the Pre-Vote algorithm described in raft thesis section
    /// 9.6. This prevents disruption when a node that has been partitioned away
    /// rejoins the cluster.
    val preVote: Boolean

    /// Choose the linearizability mode or the lease mode to read data. If you don’t care about the read consistency and want a higher read performance, you can use the lease mode.
    ///
    /// Setting this to `LeaseBased` requires `check_quorum = true`.
    val readOnlyOption: ReadOnlyOption

    /// Don't broadcast an empty raft entry to notify follower to commit an entry.
    /// This may make follower wait a longer time to apply an entry. This configuration
    /// May affect proposal forwarding and follower read.
    val skipBcastCommit: Boolean

    /// Batches every append msg if any append msg already exists
    val batchAppend: Boolean

    /// The election priority of this node.
    val priority: Long

    constructor(id: Long) {
        this.id = id
        this.heartbeatTick = 2
        this.electionTick = this.heartbeatTick * 10
        this.applied = 0
        this.maxSizePerMsg = 0
        this.maxInflightMsgs = 256
        this.checkQuorum = false
        this.preVote = false
        this.readOnlyOption = ReadOnlyOption.Safe
        this.skipBcastCommit = false
        this.batchAppend = false
        this.priority = 0
    }

    /// Runs validations against the config.
    @Throws(RaftErrorException::class)
    fun validate() {
        if (this.id == INVALID_ID) {
            raftError(RaftError.ConfigInvalid, "invalid node id")
        }

        if (this.heartbeatTick == 0) {
            raftError(RaftError.ConfigInvalid, "heartbeat tick must greater than 0")
        }

        if (this.electionTick <= this.heartbeatTick) {
            raftError(RaftError.ConfigInvalid, "election tick must be greater than heartbeat tick")
        }

        if (this.maxInflightMsgs == 0) {
            raftError(RaftError.ConfigInvalid, "max inflight messages must be greater than 0")
        }

        if (this.readOnlyOption == ReadOnlyOption.LeaseBased && !this.checkQuorum) {
            raftError(RaftError.ConfigInvalid, "read_only_option == LeaseBased requires check_quorum is true")
        }
    }

}