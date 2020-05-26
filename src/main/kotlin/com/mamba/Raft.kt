package com.mamba

import com.google.protobuf.ByteString
import com.mamba.constanst.ProgressRole
import com.mamba.constanst.ProgressState
import com.mamba.constanst.StateRole
import com.mamba.exception.RaftError
import com.mamba.exception.RaftErrorException
import com.mamba.progress.CandidacyStatus
import com.mamba.progress.Progress
import com.mamba.progress.ProgressSet
import com.mamba.storage.Storage
import eraftpb.Eraftpb
import eraftpb.Eraftpb.MessageType.*
import mu.KotlinLogging
import kotlin.collections.component1
import kotlin.collections.component2
import kotlin.math.min
import kotlin.random.Random


// CAMPAIGN_PRE_ELECTION represents the first phase of a normal election when
// Config.pre_vote is true.
val CAMPAIGN_PRE_ELECTION: ByteString = ByteString.copyFromUtf8("CampaignPreElection")

// CAMPAIGN_ELECTION represents a normal (time-based) election (the second phase
// of the election when Config.pre_vote is true).
val CAMPAIGN_ELECTION: ByteString = ByteString.copyFromUtf8("CampaignElection")

// CAMPAIGN_TRANSFER represents the type of leader transfer.
val CAMPAIGN_TRANSFER: ByteString = ByteString.copyFromUtf8("CampaignTransfer")

/// A constant represents invalid id of raft.
const val INVALID_ID: Long = 0

/// A constant represents invalid index of raft log.
const val INVALID_INDEX: Long = 0

fun buildMessage(to: Long, fieldType: Eraftpb.MessageType, from: Long?): Eraftpb.Message.Builder {
    return Eraftpb.Message.newBuilder().apply {
        this.to = to
        this.msgType = fieldType
        from?.let {
            this.from = it
        }
    }
}

/// Maps vote and pre_vote message types to their correspond responses.
fun voteRespMsgType(type: Eraftpb.MessageType): Eraftpb.MessageType = when (type) {
    MsgRequestVote -> MsgRequestVoteResponse
    MsgRequestPreVote -> MsgRequestPreVoteResponse
    else -> panic("Not a vote message: $type")
}

/// SoftState provides state that is useful for logging and debugging.
/// The state is volatile and does not need to be persisted to the WAL.
data class SoftState(
    /// The potential leader of the cluster.
    val leaderId: Long,
    /// The soft role this node may take.
    val raftState: StateRole
)

class Raft<STORAGE : Storage> {
    /// The current election term.
    var term: Long

    /// Which peer this raft is voting for.
    var vote: Long

    /// The ID of this node.
    var id: Long

    /// The current read states.
    val readStates: Vec<ReadState>

    /// The persistent log.
    var raftLog: RaftLog<STORAGE>

    /// The maximum number of messages that can be inflight.
    var maxInflight: Int

    /// The maximum length (in bytes) of all the entries.
    var maxMsgSize: Long

    /// The peer is requesting snapshot, it is the index that the follower
    /// needs it to be included in a snapshot.
    var pendingRequestSnapshot: Long

    var prs: ProgressSet

    /// The current role of this node.
    var state: StateRole

    /// Indicates whether state machine can be promoted to leader,
    /// which is true when it's a voter and its own id is in progress list.
    var promotable: Boolean

    /// The current votes for this node in an election.
    ///
    /// Reset when changing role.
    private val votes: MutableMap<Long, Boolean>

    /// The list of messages.
    val msgs: Vec<Eraftpb.Message.Builder>

    /// The leader id
    var leaderId: Long

    /// ID of the leader transfer target when its value is not None.
    ///
    /// If this is Some(id), we follow the procedure defined in raft thesis 3.10.
    var leadTransferee: Long?

    /// Only one conf change may be pending (in the log, but not yet
    /// applied) at a time. This is enforced via `pending_conf_index`, which
    /// is set to a value >= the log index of the latest pending
    /// configuration change (if any). Config changes are only allowed to
    /// be proposed if the leader's applied index is greater than this
    /// value.
    ///
    /// This value is conservatively set in cases where there may be a configuration change pending,
    /// but scanning the log is possibly expensive. This implies that the index stated here may not
    /// necessarily be a config change entry, and it may not be a `BeginMembershipChange` entry, even if
    /// we set this to one.
    var pendingConfIndex: Long

    /// The queue of read-only requests.
    var readOnly: ReadOnly

    /// Ticks since it reached last electionTimeout when it is leader or candidate.
    /// Number of ticks since it reached last electionTimeout or received a
    /// valid message from current leader when it is a follower.
    var electionElapsed: Int

    /// Number of ticks since it reached last heartbeatTimeout.
    /// only leader keeps heartbeatElapsed.
    var heartbeatElapsed: Int

    /// Whether to check the quorum
    val checkQuorum: Boolean

    /// Enable the preVote algorithm.
    ///
    /// This enables a pre-election vote round on Candidates prior to disrupting the cluster.
    ///
    /// Enable this if greater cluster stability is preferred over faster elections.
    private val preVote: Boolean

    var skipBcastCommit: Boolean
    var batchAppend: Boolean

    private val heartbeatTimeout: Int
    private val electionTimeout: Int

    // randomized_election_timeout is a random number between
    // [electionTimeout, electionTimeout * 2]. It gets reset
    // when raft changes its state to follower or candidate.
    var randomizedElectionTimeout: Int

    private val logger = KotlinLogging.logger {}

    @Throws(RaftErrorException::class)
    constructor(config: Config, store: STORAGE) {
        config.validate()
        val initialState = store.initialState()
        val confState = initialState.confState
        val voters = confState.votersList
        val learners = confState.learnersList
        this.id = config.id
        this.readStates = vec()
        this.raftLog = RaftLog(store)
        this.maxInflight = config.maxInflightMsgs
        this.maxMsgSize = config.maxSizePerMsg
        this.prs = ProgressSet(voters.size, learners.size)
        this.pendingRequestSnapshot = INVALID_INDEX
        this.state = StateRole.Follower
        this.promotable = false
        this.checkQuorum = config.checkQuorum
        this.preVote = config.preVote
        this.readOnly = ReadOnly(config.readOnlyOption)
        this.heartbeatTimeout = config.heartbeatTick
        this.electionTimeout = config.electionTick
        this.votes = HashMap()
        this.msgs = vec()
        this.leaderId = INVALID_ID
        this.leadTransferee = null
        this.term = 0
        this.electionElapsed = 0
        this.heartbeatElapsed = 0
        this.pendingConfIndex = 0
        this.vote = 0
        this.randomizedElectionTimeout = 0
        this.skipBcastCommit = config.skipBcastCommit
        this.batchAppend = config.batchAppend

        for (id in voters) {
            this.prs.insertVoterOrLearner(id, Progress(1, this.maxInflight), ProgressRole.VOTER)
            if (id == this.id) {
                this.promotable = true
            }
        }

        for (id in learners) {
            this.prs.insertVoterOrLearner(id, Progress(1, this.maxInflight), ProgressRole.LEARNER)
        }

        val applied = config.applied
        if (applied > 0) {
            this.commitApply(applied)
        }

        this.becomeFollower(this.term, INVALID_ID)

        logger.info { "raft created with id = ${config.id} term = ${this.term} " }
    }

    /// Returns whether the current raft is in lease.
    fun inLease(): Boolean = this.state == StateRole.Leader && this.checkQuorum

    /// Returns true to indicate that there will probably be some readiness need to be handled.
    fun tick(): Boolean = when (state) {
        StateRole.Follower, StateRole.Candidate, StateRole.PreCandidate -> tickElection()
        StateRole.Leader -> tickHeartbeat()
    }

    /// Run by followers and candidates after self.election_timeout.
    ///
    /// Returns true to indicate that there will probably be some readiness need to be handled.
    private fun tickElection(): Boolean {
        electionElapsed++
        if (!passElectionTimeout() || !promotable) {
            return false
        }
        electionElapsed = 0
        stepLocal(MsgHup)
        return true
    }

    // tick_heartbeat is run by leaders to send a MsgBeat after self.heartbeat_timeout.
    // Returns true to indicate that there will probably be some readiness need to be handled.
    private fun tickHeartbeat(): Boolean {
        heartbeatElapsed++
        electionElapsed++
        var hasReady = false
        if (electionElapsed >= electionTimeout) {
            electionElapsed = 0
            if (checkQuorum) {
                hasReady = true
                stepLocal(MsgCheckQuorum)
            }
            if (state == StateRole.Leader) {
                abortLeaderTransfer()
            }
        }

        if (state != StateRole.Leader) {
            return hasReady
        }

        if (heartbeatElapsed >= heartbeatTimeout) {
            heartbeatElapsed = 0
            hasReady = true
            stepLocal(MsgBeat)
        }

        return hasReady
    }

    fun step(m: Eraftpb.Message.Builder) {
        // Handle the message term, which may result in our stepping down to a follower.
        if (m.term == 0L) {
            // local message
        } else if (m.term > this.term) {
            if (m.msgType == MsgRequestVote || m.msgType == MsgRequestPreVote) {
                val force = m.context === CAMPAIGN_TRANSFER
                val inLease = this.checkQuorum &&
                        this.leaderId != INVALID_ID &&
                        this.electionElapsed < this.electionTimeout
                if (!force && inLease) {
                    // if a server receives RequestVote request within the minimum election
                    // timeout of hearing from a current leader, it does not update its term
                    // or grant its vote
                    //
                    // This is included in the 3rd concern for Joint Consensus, where if another
                    // peer is removed from the cluster it may try to hold elections and disrupt
                    // stability.
                    return
                }
            }
            if (m.msgType == MsgRequestPreVote || (m.msgType == MsgRequestPreVoteResponse && !m.reject)) {
                // For a pre-vote request:
                // Never change our term in response to a pre-vote request.
                //
                // For a pre-vote response with pre-vote granted:
                // We send pre-vote requests with a term in our future. If the
                // pre-vote is granted, we will increment our term when we get a
                // quorum. If it is not, the term comes from the node that
                // rejected our vote so we should become a follower at the new
                // term.
            } else {
                logger.info { "received a message with higher term from ${m.from} [term: ${this.term}, message term: ${m.term}, msg type: ${m.msgType}]" }
                val from = when (m.msgType) {
                    MsgAppend, MsgHeartbeat, MsgSnapshot -> m.from
                    else -> INVALID_ID
                }
                becomeFollower(m.term, from)
            }
        } else if (m.term < this.term) {
            if ((this.checkQuorum || this.preVote) && (m.msgType == MsgHeartbeat || m.msgType == MsgAppend)) {
                // We have received messages from a leader at a lower term. It is possible
                // that these messages were simply delayed in the network, but this could
                // also mean that this node has advanced its term number during a network
                // partition, and it is now unable to either win an election or to rejoin
                // the majority on the old term. If checkQuorum is false, this will be
                // handled by incrementing term numbers in response to MsgVote with a higher
                // term, but if checkQuorum is true we may not advance the term on MsgVote and
                // must generate other messages to advance the term. The net result of these
                // two features is to minimize the disruption caused by nodes that have been
                // removed from the cluster's configuration: a removed node will send MsgVotes
                // which will be ignored, but it will not receive MsgApp or MsgHeartbeat, so it
                // will not create disruptive term increases, by notifying leader of this node's
                // activeness.
                // The above comments also true for Pre-Vote
                //
                // When follower gets isolated, it soon starts an election ending
                // up with a higher term than leader, although it won't receive enough
                // votes to win the election. When it regains connectivity, this response
                // with "pb.MsgAppResp" of higher term would force leader to step down.
                // However, this disruption is inevitable to free this stuck node with
                // fresh election. This can be prevented with Pre-Vote phase.
                val toSend = buildMessage(m.from, MsgAppendResponse, null)
                this.send(toSend)
            } else if (m.msgType == MsgRequestPreVote) {
                // Before pre_vote enable, there may be a recieving candidate with higher term,
                // but less log. After update to pre_vote, the cluster may deadlock if
                // we drop messages with a lower term.
                val toSend = buildMessage(m.from, MsgRequestPreVoteResponse, null).apply {
                    this.term = this.term
                    this.reject = true
                }
                this.send(toSend)
            } else {
                // ignore other cases
                logger.info { "ignored a message with lower term from ${m.from} [term: ${this.term}, message term: ${m.term}, msg type: ${m.msgType}]" }
            }
            return
        }

        when (m.msgType) {
            MsgHup -> this.hup(false)
            MsgRequestVote, MsgRequestPreVote -> {
                // We can vote if this is a repeat of a vote we've already cast...
                val canVote = ((this.vote == m.from) ||
                        // ...we haven't voted and we don't think there's a leader yet in this term...
                        (this.vote == INVALID_ID && this.leaderId == INVALID_ID) ||
                        // ...or this is a PreVote for a future term...
                        (m.msgType == MsgRequestPreVote && m.term > this.term)) &&
                        // raft log must up to date
                        this.raftLog.isUpToDate(m.index, m.logTerm)
                if (canVote) {
                    // When responding to Msg{Pre,}Vote messages we include the term
                    // from the message, not the local term. To see why consider the
                    // case where a single node was previously partitioned away and
                    // it's local term is now of date. If we include the local term
                    // (recall that for pre-votes we don't update the local term), the
                    // (pre-)campaigning node on the other end will proceed to ignore
                    // the message (it ignores all out of date messages).
                    // The term in the original message and current local term are the
                    // same in the case of regular votes, but different for pre-votes.
                    logger.info {
                        "[log term: {log_term}, index: {log_index}, vote: ${this.vote}] cast vote from ${m.from} " +
                                "[log term: ${m.logTerm}, index: ${m.index}, msg type: ${m.msgType}] at term ${this.term}"
                    }
                    if (m.msgType == MsgRequestVote) {
                        // Only record real votes.
                        this.electionElapsed = 0
                        this.vote = m.from
                    }
                } else {
                    logger.info {
                        "[log term: {log_term}, index: {log_index}, vote: ${this.vote}] rejected vote from ${m.from} " +
                                "[log term: ${m.logTerm}, index: ${m.index}, msg type: ${m.msgType}] at term ${this.term}"
                    }
                }

                // send back vote result to candidate
                val toSend = buildMessage(
                    m.from,
                    voteRespMsgType(m.msgType),
                    null
                ).apply {
                    this.reject = !canVote
                    this.term = m.term
                }
                this.send(toSend)
            }
            else -> when (this.state) {
                StateRole.Leader -> this.stepLeader(m)
                StateRole.Follower -> this.stepFollower(m)
                StateRole.Candidate, StateRole.PreCandidate -> this.stepCandidate(m)
            }
        }
    }

    // step_candidate is shared by state Candidate and PreCandidate; the difference is
    // whether they respond to MsgRequestVote or MsgRequestPreVote.
    private fun stepCandidate(m: Eraftpb.Message.Builder) {
        when (m.msgType) {
            MsgPropose -> {
                logger.info { "no leader at term $term; dropping proposal" }
                raftError(RaftError.ProposalDropped)
            }
            MsgAppend -> {
                this.becomeFollower(m.term, m.from)
                this.handleAppendEntries(m)
            }
            MsgHeartbeat -> {
                this.becomeFollower(m.term, m.from)
                this.handleHeartbeat(m)
            }
            MsgSnapshot -> {
                this.becomeFollower(m.term, m.from)
                this.handleSnapshot(m)
            }
            MsgRequestVoteResponse, MsgRequestPreVoteResponse -> {
                // Only handle vote responses corresponding to our candidacy (while in
                // state Candidate, we may get stale MsgPreVoteResp messages in this term from
                // our pre-candidate state).
                if ((this.state == StateRole.PreCandidate && m.msgType != MsgRequestPreVoteResponse) ||
                    (this.state == StateRole.Candidate && m.msgType != MsgRequestVoteResponse)
                ) {
                    return
                }

                val acceptance = !m.reject
                logger.info { "received ${if (acceptance) "rejection" else "acceptance"} from ${m.from}" }

                this.registerVote(m.from, acceptance)
                this.handleCandidacyStatus()
            }
            MsgTimeoutNow -> {
                if (logger.isDebugEnabled) {
                    logger.debug { "${this.term} ignored MsgTimeoutNow from ${m.from}" }
                }
            }
            else -> { }
        }
    }

    private fun handleSnapshot(m: Eraftpb.Message.Builder) {
        val restored = this.restore(m.snapshot)

        val metadata = m.snapshot.metadata
        logger.info {
            "[commit: ${this.raftLog.committed}, term: ${this.term}] ${if (restored) "restored" else "ignored"} snapshot " +
                    "[index: ${metadata.index}, term: ${metadata.term}]"
        }

        val lastIdx = if (restored) this.raftLog.lastIndex() else this.raftLog.committed

        Eraftpb.Message.newBuilder().run {
            this.msgType = MsgAppendResponse
            this.to = m.from
            this.index = lastIdx
            this@Raft.send(this)
        }
    }

    /// Recovers the state machine from a snapshot. It restores the log and the
    /// configuration of state machine.
    private fun restore(snapshot: Eraftpb.Snapshot): Boolean {
        if (snapshot.metadata.index < this.raftLog.committed) {
            return false
        }
        val res = this.restoreRaft(snapshot)
        if (res != null) {
            return res
        }
        this.raftLog.restore(snapshot)
        return true
    }

    private fun restoreRaft(snapshot: Eraftpb.Snapshot): Boolean? {
        val metadata = snapshot.metadata

        // Do not fast-forward commit if we are requesting snapshot.
        if (this.pendingRequestSnapshot == INVALID_INDEX && this.raftLog.matchTerm(metadata.index, metadata.term)) {
            logger.info {
                "[commit: ${this.raftLog.committed}, last index: ${this.raftLog.lastIndex()}, last term: ${this.raftLog.lastTerm()}] " +
                        "fast-forwarded commit to snapshot [index: ${metadata.index}, term: ${metadata.term}]"
            }
            this.raftLog.commitTo(metadata.index)
            return false
        }

        // After the Raft is initialized, a voter can't become a learner any more.
        if (this.prs.progress.isNotEmpty() && this.promotable) {
            for (id in metadata.confState.learnersList) {
                if (id == this.id) {
                    logger.error { "can't become learner when restores snapshot(index: ${metadata.index}, term: ${metadata.term})" }
                    return false
                }
            }
        }

        logger.info {
            "[commit: ${this.raftLog.committed}, last index: ${this.raftLog.lastIndex()}, last term: ${this.raftLog.lastTerm()}] " +
                    "starts to restore snapshot [index: ${metadata.index}, term: ${metadata.term}]"
        }

        // Restore progress set and the learner flag.
        val nextIdx = this.raftLog.lastIndex() + 1

        this.prs.restoreSnapshotMeta(metadata, nextIdx, this.maxInflight)
        this.prs.progress[this.id]!!.matched = nextIdx - 1
        if (this.prs.configuration.votes.contains(this.id)) {
            this.promotable = true
        } else if (this.prs.configuration.learners.contains(this.id)) {
            this.promotable = false
        }

        this.pendingRequestSnapshot = INVALID_INDEX
        return null
    }

    /// For a message, commit and send out heartbeat.
    private fun handleHeartbeat(m: Eraftpb.Message.Builder) {
        this.raftLog.commitTo(m.commit)
        if (this.pendingRequestSnapshot != INVALID_INDEX) {
            this.sendRequestSnapshot()
            return
        }

        Eraftpb.Message.newBuilder().run {
            this.msgType = MsgHeartbeatResponse
            this.to = m.from
            this.context = m.context
            this@Raft.send(this)
        }
    }

    private fun stepFollower(m: Eraftpb.Message.Builder) {
        when (m.msgType) {
            MsgPropose -> {
                if (this.leaderId == INVALID_ID) {
                    logger.info { "no leader at term ${this.term}; dropping proposal" }
                    raftError(RaftError.ProposalDropped)
                }
                // forwarding to leader
                m.to = this.leaderId
                this.send(m)
            }
            MsgAppend -> {
                this.electionElapsed = 0
                this.leaderId = m.from
                this.handleAppendEntries(m)
            }
            MsgHeartbeat -> {
                this.electionElapsed = 0
                this.leaderId = m.from
                this.handleHeartbeat(m)
            }
            MsgSnapshot -> {
                this.electionElapsed = 0
                this.leaderId = m.from
                this.handleSnapshot(m)
            }
            MsgTransferLeader -> {
                if (this.leaderId == INVALID_ID) {
                    logger.info { "no leader at term ${this.term}; dropping leader transfer msg" }
                    return
                }
                m.to = this.leaderId
                this.send(m)
            }
            MsgTimeoutNow -> {
                if (!this.promotable) {
                    logger.info { "received MsgTimeoutNow from ${m.from} but is not promotable" }
                    return
                }

                logger.info { "[term ${this.term}] received MsgTimeoutNow from ${m.from} and starts an election to get leadership." }
                // Leadership transfers never use pre-vote even if self.pre_vote is true; we
                // know we are not recovering from a partition so there is no need for the
                // extra round trip.
                this.hup(true)
            }
            MsgReadIndex -> {
                if (this.leaderId == INVALID_ID) {
                    logger.info { "no leader at term ${this.term}; dropping index reading msg" }
                    return
                }
                m.to = this.leaderId
                this.send(m)
            }
            MsgReadIndexResp -> {
                if (m.entriesCount != 1) {
                    logger.error { "invalid format of MsgReadIndexResp from ${m.from} entries count ${m.entriesCount}" }
                    return
                }
                val rs = ReadState(
                    index = m.index,
                    requestCtx = m.entriesList[0].data
                )
                this.readStates.add(rs)
                // `index` and `term` in MsgReadIndexResp is the leader's commit index and its current term,
                // the log entry in the leader's commit index will always have the leader's current term,
                // because the leader only handle MsgReadIndex after it has committed log entry in its term.
                this.raftLog.maybeCommit(m.index, m.term)
            }
            else -> { }
        }
    }

    private fun stepLeader(m: Eraftpb.Message.Builder) {
        // These message types do not require any progress for m.From.
        when (m.msgType) {
            MsgBeat -> {
                this.bcastHeartbeat()
                return
            }
            MsgCheckQuorum -> {
                if (!this.checkQuorumActive()) {
                    logger.warn { "stepped down to follower since quorum is not active" }
                    this.becomeFollower(this.term, INVALID_ID)
                }
                return
            }
            MsgPropose -> {
                if (m.entriesCount == 0) {
                    fatal(logger, "stepped empty MsgProp")
                }
                if (!this.prs.voterIds().contains(this.id)) {
                    // If we are not currently a member of the range (i.e. this node
                    // was removed from the configuration while serving as leader),
                    // drop any new proposals.
                    raftError(RaftError.ProposalDropped)
                }

                if (this.leadTransferee != null) {
                    if (logger.isDebugEnabled) {
                        logger.debug { "[term ${this.term}] transfer leadership to $this.leadTransferee is in progress; dropping proposal" }
                    }
                    raftError(RaftError.ProposalDropped)
                }

                for ((i, e) in m.entriesBuilderList.withIndex()) {
                    if (e.entryType == Eraftpb.EntryType.EntryConfChange) {
                        if (this.hasPendingConf()) {
                            logger.info { "propose conf entry ignored since pending unapplied configuration" }
                            val entry = Eraftpb.Entry.newBuilder().apply { this.entryType = Eraftpb.EntryType.EntryNormal }
                            m.setEntries(i, entry)
                        } else {
                            this.pendingConfIndex = this.raftLog.lastIndex() + i + 1
                        }
                    }
                }
                this.appendEntry(m.entriesBuilderList.toTypedArray())
                this.bcastAppend()
                return
            }
            MsgReadIndex -> {
                if (!this.commitToCurrentTerm()) {
                    // Reject read only request when this leader has not committed any log entry
                    // in its term.
                    return
                }

                if (!this.prs.hasQuorum(this.id) && this.readOnly.option == ReadOnlyOption.Safe) {
                    // thinking: use an interally defined context instead of the user given context.
                    // We can express this in terms of the term and index instead of
                    // a user-supplied value.
                    // This would allow multiple reads to piggyback on the same message.
                    val ctx = m.entriesList.first().data
                    this.readOnly.addRequest(this.raftLog.committed, this.id, m)
                    this.bcastHeartbeatWithCtx(ctx)
                } else {
                    // there is only one voting member (the leader) in the cluster or LeaseBased Read
                    this.responseToReadIndexReq(m, this.raftLog.committed)
                }
                return
            }
            else -> { /* to do nothing */ }
        }

        val from = this.prs.progress[m.from]
        if (from == null) {
            if (logger.isDebugEnabled) {
                logger.debug("no progress available for ${m.from}")
            }
            return
        }

        when (m.msgType) {
            MsgAppendResponse -> {
                from.recentActive = true
                if (m.reject) {
                    if (logger.isDebugEnabled) {
                        logger.debug("received msgAppend rejection last index ${m.rejectHint}, from ${m.from}, index ${m.index}")
                    }

                    if (from.maybeDecrTo(m.index, m.rejectHint, m.requestSnapshot)) {
                        if (logger.isDebugEnabled) {
                            logger.debug("decreased progress of ${m.from}")
                        }
                        if (from.state == ProgressState.Replicate) {
                            from.becomeProbe()
                        }
                        this.sendAppend(m.from, from)
                    }
                } else {
                    val oldPaused = from.isPaused()
                    if (!from.maybeUpdate(m.index)) {
                        return
                    }

                    when (from.state) {
                        ProgressState.Probe -> from.becomeReplicate()
                        ProgressState.Snapshot -> if (from.maybeSnapshotAbort()) {
                            if (logger.isDebugEnabled) {
                                logger.debug("snapshot aborted, resumed sending replication messages to {}")
                            }
                            from.becomeProbe()
                        }
                        ProgressState.Replicate -> from.ins.freeTo(m.index)
                    }

                    if (this.maybeCommit()) {
                        this.bcastAppend()
                    } else if (oldPaused) {
                        // If we were paused before, this node may be missing the
                        // latest commit index, so send it.
                        this.sendAppend(m.from, from)
                    }

                    // We've updated flow control information above, which may
                    // allow us to send multiple (size-limited) in-flight messages
                    // at once (such as when transitioning from probe to
                    // replicate, or when freeTo() covers multiple messages). If
                    // we have more entries to send, send as many messages as we
                    // can (without sending empty messages for the commit index)
                    @Suppress("ControlFlowWithEmptyBody")
                    while (this.maybeSendAppend(m.from, from, false)) {
                    }

                    // Transfer leadership is in progress.
                    val isTimeoutNow = this.leadTransferee != null &&
                            this.leadTransferee == m.from &&
                            from.matched == this.raftLog.lastIndex()
                    if (isTimeoutNow) {
                        logger.info("sent MsgTimeoutNow to ${m.from} after received MsgAppResp")
                        this.sendTimeoutNow(m.from)
                    }
                }
            }
            MsgHeartbeatResponse -> {
                from.recentActive = true
                from.resume()

                // free one slot for the full inflights window to allow progress.
                if (from.state == ProgressState.Replicate && from.ins.full()) {
                    from.ins.freeFirstOne()
                }

                // Does it request snapshot?
                if (from.matched < this.raftLog.lastIndex() || from.pendingRequestSnapshot != INVALID_INDEX) {
                    this.sendAppend(m.from, from)
                }

                if (this.readOnly.option != ReadOnlyOption.Safe) {
                    return
                }

                // maybe have need to handle readIndex response
                this.handleReadIndexAdvance(m.from, m.context)
            }
            MsgSnapStatus -> {
                if (from.state == ProgressState.Snapshot) {
                    if (logger.isDebugEnabled) {
                        logger.debug("snapshot ${if (m.reject) "failed" else "succeeded"}, resumed sending replication messages to ${m.from}")
                    }
                    if (m.reject) {
                        from.snapshotFailure()
                    }
                    from.becomeProbe()
                    // If snapshot finish, wait for the msgAppResp from the remote node before sending
                    // out the next msgAppend.
                    // If snapshot failure, wait for a heartbeat interval before next try
                    from.pause()
                    from.pendingRequestSnapshot = INVALID_INDEX
                }
            }
            MsgUnreachable -> {
                // During optimistic replication, if the remote becomes unreachable,
                // there is huge probability that a MsgAppend is lost.
                if (from.state == ProgressState.Replicate) {
                    from.becomeProbe()
                }
                if (logger.isDebugEnabled) {
                    logger.debug("failed to send message to ${m.from} because it is unreachable")
                }
            }
            MsgTransferLeader -> {
                val leadTransferee = m.from
                if (this.prs.learnerIds().contains(leadTransferee)) {
                    if (logger.isDebugEnabled) {
                        logger.debug("ignored transferring leadership")
                    }
                    return
                }
                val lastLeadTransferee = this.leadTransferee
                if (lastLeadTransferee != null) {
                    if (lastLeadTransferee == leadTransferee) {
                        logger.info("[term ${this.term}] transfer leadership to $leadTransferee is in progress, ignores request to same node")
                        return
                    }
                    this.abortLeaderTransfer()
                    logger.info("[term ${this.term}] abort previous transferring leadership to $lastLeadTransferee")
                }

                if (leadTransferee == this.id) {
                    if (logger.isDebugEnabled) {
                        logger.debug("already leader; ignored transferring leadership to self")
                    }
                    return
                }
                // Transfer leadership to third party.
                logger.info("[term ${this.term}] starts to transfer leadership to $leadTransferee")
                // Transfer leadership should be finished in one electionTimeout
                // so reset r.electionElapsed.
                this.electionElapsed = 0
                this.leadTransferee = leadTransferee

                val pr = this.prs.progress[leadTransferee]
                if (pr!!.matched == this.raftLog.lastIndex()) {
                    logger.info("sends MsgTimeoutNow to $leadTransferee immediately as $leadTransferee already has up-to-date log")
                    this.sendTimeoutNow(leadTransferee)
                } else {
                    this.sendAppend(leadTransferee, pr)
                }
            }
            else -> { /* to do nothing */ }
        }
    }

    private fun bcastHeartbeatWithCtx(ctx: ByteString?) {
        for ((id, pr) in this.prs.progress) {
            if (id == this.id) {
                continue
            }
            this.sendHeartbeat(id, pr, ctx)
        }
    }

    // send_heartbeat sends an empty MsgAppend
    private fun sendHeartbeat(to: Long, pr: Progress, ctx: ByteString?) {
        // Attach the commit as min(to.matched, self.raft_log.committed).
        // When the leader sends out heartbeat message,
        // the receiver(follower) might not be matched with the leader
        // or it might not have all the committed entries.
        // The leader MUST NOT forward the follower's commit to
        // an unmatched index.
        Eraftpb.Message.newBuilder().run {
            this.to = to
            this.msgType = MsgHeartbeat
            this.commit = min(pr.matched, this@Raft.raftLog.committed)
            ctx?.let {
                this.context = it
            }
            this@Raft.send(this)
        }
    }

    private fun hasPendingConf(): Boolean = this.pendingConfIndex > this.raftLog.applied

    /// Sends RPC, without entries to all the peers.
    private fun bcastHeartbeat() {
        this.bcastHeartbeatWithCtx(this.readOnly.lastPendingRequestCtx())
    }

    private fun hup(transferLeader: Boolean) {
        if (this.state == StateRole.Leader) {
            logger.isDebugEnabled
            logger.debug { "ignoring MsgHup because already leader" }
            return
        }
        // If there is a pending snapshot, its index will be returned by
        // `maybe_first_index`. Note that snapshot updates configuration
        // already, so as long as pending entries don't contain conf change
        // it's safe to start campaign.
        val firstIdx = this.raftLog.unstable.maybeFirstIndex() ?: this.raftLog.applied + 1

        val entries = try {
            this.raftLog.slice(firstIdx, this.raftLog.committed + 1, null)
        } catch (e: RaftErrorException) {
            fatal(
                logger,
                "unexpected error getting unapplied entries [$firstIdx, ${this.raftLog.committed + 1}): ${e.error}"
            )
        }

        val n = this.numPendingConf(entries)
        if (n != 0) {
            logger.warn { "cannot campaign at term ${this.term} since there are still $n pending configuration changes to apply" }
            return
        }

        logger.info { "starting a new election [term: ${this.term}]" }

        when {
            transferLeader -> this.campaign(CAMPAIGN_TRANSFER)
            this.preVote -> this.campaign(CAMPAIGN_PRE_ELECTION)
            else -> this.campaign(CAMPAIGN_ELECTION)
        }
    }

    /// Campaign to attempt to become a leader.
    ///
    /// If pre vote is enabled, this is handled as well.
    private fun campaign(campaignType: ByteString) {
        val (voteType, term) = if (campaignType == CAMPAIGN_PRE_ELECTION) {
            this.becomePreCandidate()
            Pair(MsgRequestPreVote, this.term + 1)
        } else {
            this.becomeCandidate()
            Pair(MsgRequestVote, this.term)
        }

        this.registerVote(this.id, true)

        if (handleCandidacyStatus() == CandidacyStatus.Elected) {
            // We won the election after voting for ourselves (which must mean that
            // this is a single-node cluster).
            return
        }

        // Only send vote request to voters.
        for (id in this.prs.voterIds()) {
            if (id == this.id) {
                // skip self
                continue
            }

            logger.info { "[log term: ${this.raftLog.lastTerm()}, index: ${this.raftLog.lastIndex()}] sent request to $id" }

            val toSend = buildMessage(id, voteType, null).apply {
                this.term = term
                this.index = this@Raft.raftLog.lastIndex()
                this.logTerm = this@Raft.raftLog.lastTerm()
                if (campaignType === CAMPAIGN_TRANSFER) {
                    this.context = campaignType
                }
            }
            this.send(toSend)
        }
    }

    /// Check if it can become leader.
    private fun handleCandidacyStatus(): CandidacyStatus = this.prs.candidacyStatus(this.votes).also {
        when (it) {
            CandidacyStatus.Elected -> {
                if (this.state == StateRole.PreCandidate) {
                    this.campaign(CAMPAIGN_ELECTION)
                } else {
                    this.becomeLeader()
                    this.bcastAppend()
                }
            }
            CandidacyStatus.Ineligible -> {
                // pb.MsgPreVoteResp contains future term of pre-candidate
                // m.term > self.term; reuse self.term
                this.becomeFollower(this.term, INVALID_ID)
            }
            CandidacyStatus.Eligible -> {
            }
        }
    }

    /// Sends RPC, with entries to all peers that are not up-to-date
    /// according to the progress recorded in r.prs().
    private fun bcastAppend() {
        for ((pid, pr) in this.prs.progress) {
            if (pid == this.id) {
                continue
            }
            this.sendAppend(pid, pr)
        }
    }

    /// Sends an append RPC with new entries to the given peer,
    /// if necessary. Returns true if a message was sent. The allow_empty
    /// argument controls whether messages with no entries will be sent
    /// ("empty" messages are useful to convey updated Commit indexes, but
    /// are undesirable when we're sending multiple messages in a batch).
    private fun maybeSendAppend(to: Long, pr: Progress, allowEmpty: Boolean): Boolean {
        if (pr.isPaused()) {
            if (logger.isTraceEnabled) {
                logger.trace { "Skipping sending to $to, it's paused" }
            }
            return false
        }
        val m = Eraftpb.Message.newBuilder().apply { this.to = to }
        if (pr.pendingRequestSnapshot != INVALID_INDEX) {
            // Check pending request snapshot first to avoid unnecessary loading entries.
            if (!this.prepareSendSnapshot(m, pr, to)) {
                return false
            }
        } else {
            try {
                val entries = this.raftLog.entries(pr.nextIdx, this.maxMsgSize)

                if (!allowEmpty && entries.isEmpty()) {
                    return false
                }

                val term = this.raftLog.term(pr.nextIdx - 1)

                if (this.batchAppend && this.tryBatching(to, pr, entries)) {
                    return true
                }
                this.prepareSendEntries(m, pr, term, entries)
            } catch (e: RaftErrorException) {
                // send snapshot if we failed to get term or entries.
                if (!this.prepareSendSnapshot(m, pr, to)) {
                    return false
                }
            }
        }
        this.send(m)
        return true
    }

    private fun prepareSendEntries(m: Eraftpb.Message.Builder, pr: Progress, term: Long, entries: Vec<Eraftpb.Entry>) {
        with(m) {
            msgType = MsgAppend
            index = pr.nextIdx - 1
            logTerm = term
            addAllEntries(entries)
            commit = this@Raft.raftLog.committed
            if (entriesCount > 0) {
                pr.updateState(entriesList.last().index)
            }
        }
    }

    private fun tryBatching(to: Long, pr: Progress, entries: Vec<Eraftpb.Entry>): Boolean {
        // if MsgAppend for the receiver already exists, try_batching
        // will append the entries to the existing MsgAppend
        var batched = false
        for (msg in this.msgs) {
            if (msg.msgType == MsgAppend && msg.to == to) {
                if (!entries.isEmpty()) {
                    if (isContinuousEntries(msg, entries)) {
                        return batched
                    }
                    // merge entries
                    msg.entriesList.addAll(entries)

                    pr.updateState(msg.entriesList.last().index)
                }
                msg.commit = this.raftLog.committed
                batched = true
                break
            }
        }
        return batched
    }

    /// Sends an append RPC with new entries (if any) and the current commit index to the given
    /// peer.
    private fun sendAppend(to: Long, pr: Progress) {
        this.maybeSendAppend(to, pr, true)
    }

    private fun prepareSendSnapshot(m: Eraftpb.Message.Builder, pr: Progress, to: Long): Boolean {
        if (!pr.recentActive) {
            if (logger.isDebugEnabled) {
                logger.debug { "ignore sending snapshot to $to since it is not recently active" }
            }
            return false
        }
        m.msgType = MsgSnapshot
        try {
            val raftSnapshot = this.raftLog.snapshot(pr.pendingRequestSnapshot)
            if (raftSnapshot.metadata.index == 0L) {
                fatal(logger, "need non-empty snapshot")
            }
            m.snapshot = raftSnapshot
            if (logger.isDebugEnabled) {
                logger.debug {
                    "[first index: ${this.raftLog.firstIndex()}, commit: ${this.raftLog.committed}] " +
                            "sent snapshot[index: ${raftSnapshot.metadata.index}, term: ${raftSnapshot.metadata.term}] to $to"
                }
            }

            pr.becomeSnapshot(raftSnapshot.metadata.index)

            if (logger.isDebugEnabled) {
                logger.debug { "paused sending replication messages to $to" }
            }
            return true
        } catch (e: RaftErrorException) {
            if (e.error == RaftError.Storage_SnapshotTemporarilyUnavailable) {
                if (logger.isDebugEnabled) {
                    logger.debug { "failed to send snapshot to $to because snapshot is temporarily unavailable" }
                }
                return false
            }
            fatal(logger, "unexpected error: ${e.error.name}")
        }
    }

    /// Makes this raft the leader.
    ///
    /// # Panics
    ///
    /// Panics if this is a follower node.
    private fun becomeLeader() {
        if (logger.isTraceEnabled) {
            logger.trace { "ENTER become_leader" }
        }
        if (this.state == StateRole.Follower) {
            panic("invalid transition [follower -> leader]")
        }
        val term = this.term
        this.reset(term)
        this.leaderId = this.id
        this.state = StateRole.Leader

        // Followers enter replicate mode when they've been successfully probed
        // (perhaps after having received a snapshot as a result). The leader is
        // trivially in this state. Note that r.reset() has initialized this
        // progress with the last index already.
        this.prs.progress[this.id]!!.becomeReplicate()

        // Conservatively set the pending_conf_index to the last index in the
        // log. There may or may not be a pending config change, but it's
        // safe to delay any future proposals until we commit all our
        // pending log entries, and scanning the entire tail of the log
        // could be expensive.
        this.pendingConfIndex = this.raftLog.lastIndex()

        this.appendEntry(emptyArray())

        logger.info { "became leader at term $term" }

        if (logger.isTraceEnabled) {
            logger.trace { "EXIT become_leader" }
        }
    }

    /// Resets the current node to a given term.
    private fun reset(term: Long) {
        if (this.term != term) {
            this.term = term
            this.vote = INVALID_ID
        }

        this.leaderId = INVALID_ID
        this.resetRandomizedElectionTimeout()
        this.electionElapsed = 0
        this.heartbeatElapsed = 0

        this.abortLeaderTransfer()

        this.votes.clear()

        this.pendingConfIndex = 0
        this.readOnly = ReadOnly(this.readOnly.option)
        this.pendingRequestSnapshot = INVALID_INDEX

        val lastIdx = this.raftLog.lastIndex()
        for ((id, p) in this.prs.progress) {
            p.reset(lastIdx + 1)
            if (id == this.id) {
                p.matched = lastIdx
            }
        }
    }

    /// Appends a slice of entries to the log. The entries are updated to match
    /// the current index and term.
    private fun appendEntry(entries: Array<Eraftpb.Entry.Builder>) {
        val lastIdx = this.raftLog.lastIndex()

        for ((i, e) in entries.withIndex()) {
            e.term = this.term
            e.index = lastIdx + i + 1
        }

        // use latest "last" index after truncate/append
        val latestLastIdx = this.raftLog.append(entries)

        this.prs.progress[this.id]!!.maybeUpdate(latestLastIdx)

        // Regardless of maybe_commit's return, our caller will call bcastAppend.
        this.maybeCommit()
    }

    /// Attempts to advance the commit index. Returns true if the commit index
    /// changed (in which case the caller should call `r.bcast_append`).
    private fun maybeCommit(): Boolean {
        val mci = this.prs.maximalCommittedIndex()
        return this.raftLog.maybeCommit(mci, this.term)
    }

    /// Regenerates and stores the election timeout.
    private fun resetRandomizedElectionTimeout() {
        val prevTimeout = this.randomizedElectionTimeout

        val timeout = this.electionTimeout + Random.nextInt(this.electionTimeout)

        if (logger.isDebugEnabled) {
            this.logger.debug { "reset election timeout $prevTimeout -> $timeout at ${this.electionElapsed}" }
        }

        this.randomizedElectionTimeout = timeout
    }

    // check_quorum_active returns true if the quorum is active from
    // the view of the local raft state machine. Otherwise, it returns
    // false.
    // check_quorum_active also resets all recent_active to false.
    // check_quorum_active can only called by leader.
    private fun checkQuorumActive(): Boolean = this.prs.quorumRecentlyActive(this.id)

    /// Issues a message to timeout immediately.
    private fun sendTimeoutNow(to: Long) {
        val toSend = buildMessage(to, MsgTimeoutNow, null)
        this.send(toSend)
    }

    /// Sets the vote of `id` to `vote`.
    private fun registerVote(id: Long, acceptance: Boolean) {
        this.votes.putIfAbsent(id, acceptance)
    }

    /// Converts this node to a candidate
    ///
    /// # Panics
    ///
    /// Panics if a leader already exists.
    private fun becomeCandidate() {
        if (this.state == StateRole.Leader) {
            panic("invalid transition [leader -> candidate]")
        }
        this.reset(this.term + 1)
        this.vote = this.id
        this.state = StateRole.Candidate
        logger.info { "became candidate at term ${this.term}" }
    }

    /// Converts this node to a pre-candidate
    ///
    /// # Panics
    ///
    /// Panics if a leader already exists.
    private fun becomePreCandidate() {
        if (this.state == StateRole.Leader) {
            panic("invalid transition [leader -> pre-candidate]")
        }
        this.votes.clear()
        // Becoming a pre-candidate changes our state.
        // but doesn't change anything else. In particular it does not increase
        // self.term or change self.vote.
        this.state = StateRole.PreCandidate
        // If a network partition happens, and leader is in minority partition,
        // it will step down, and become follower without notifying others.
        this.leaderId = INVALID_ID
        logger.info { "became pre-candidate at term ${this.term}" }
    }

    private fun numPendingConf(entries: Vec<Eraftpb.Entry>): Int =
        entries.count { it.entryType == Eraftpb.EntryType.EntryConfChange }

    // send persists state to stable storage and then sends to its mailbox.
    private fun send(m: Eraftpb.Message.Builder) {
        if (logger.isDebugEnabled) {
            logger.debug { "Sending from ${this.id} to ${m.to}" }
        }
        when (m.msgType) {
            MsgRequestVote, MsgRequestPreVote, MsgRequestVoteResponse, MsgRequestPreVoteResponse -> {
                if (m.term == 0L) {
                    // All {pre-,}campaign messages need to have the term set when
                    // sending.
                    // - MsgVote: m.Term is the term the node is campaigning for,
                    //   non-zero as we increment the term when campaigning.
                    // - MsgVoteResp: m.Term is the new r.Term if the MsgVote was
                    //   granted, non-zero for the same reason MsgVote is
                    // - MsgPreVote: m.Term is the term the node will campaign,
                    //   non-zero as we use m.Term to indicate the next term we'll be
                    //   campaigning for
                    // - MsgPreVoteResp: m.Term is the term received in the original
                    //   MsgPreVote if the pre-vote was granted, non-zero for the
                    //   same reasons MsgPreVote is
                    fatal(logger, "term should be set when sending ${m.msgType.name}")
                }
            }
            else -> {
                if (m.term != 0L) {
                    fatal(logger, "term should not be set when sending ${m.msgType.name} (was ${m.term})")
                }
                // do not attach term to MsgPropose, MsgReadIndex
                // proposals are a way to forward to the leader and
                // should be treated as local message.
                // MsgReadIndex is also forwarded to leader.
                if ((m.msgType != MsgPropose) && (m.msgType != MsgReadIndex)) {
                    m.term = this.term
                }
            }
        }
        m.from = this.id
        this.msgs.add(m)
    }

    /// Converts this node to a follower.
    private fun becomeFollower(term: Long, leaderId: Long) {
        val oldPendingRequestSnapshot = this.pendingRequestSnapshot
        this.reset(term)
        this.leaderId = leaderId
        this.state = StateRole.Follower
        this.pendingRequestSnapshot = oldPendingRequestSnapshot

        logger.info { "became follower at term ${this.term}" }
    }

    private fun stepLocal(messageType: Eraftpb.MessageType) {
        step(buildMessage(INVALID_ID, messageType, this.id))
    }

    /// For a given message, append the entries to the log.
    private fun handleAppendEntries(m: Eraftpb.Message.Builder) {
        if (this.pendingRequestSnapshot != INVALID_INDEX) {
            this.sendRequestSnapshot()
            return
        }

        if (m.index < this.raftLog.committed) {
            if (logger.isDebugEnabled) {
                logger.debug { "got message with lower index than committed." }
            }
            Eraftpb.Message.newBuilder().run {
                this.msgType = MsgAppendResponse
                this.to = m.from
                this.index = this@Raft.raftLog.committed
                this@Raft.send(this)
            }
            return
        }

        Eraftpb.Message.newBuilder().run {
            this.to = m.from
            this.msgType = MsgAppendResponse
            val lastIdx =
                this@Raft.raftLog.maybeAppend(m.index, m.logTerm, m.commit, m.entriesBuilderList.toTypedArray())
            if (lastIdx != null) {
                this.index = lastIdx
            } else {
                if (logger.isDebugEnabled) {
                    logger.debug { "rejected msgApp [log term: ${m.logTerm}, index: ${m.index}] from ${m.from}" }
                }
                this.index = m.index
                this.reject = true
                this.rejectHint = this@Raft.raftLog.lastIndex()
            }
            this@Raft.send(this)
        }
    }

    private fun sendRequestSnapshot() {
        Eraftpb.Message.newBuilder().run {
            this.msgType = MsgAppendResponse
            this.index = this@Raft.raftLog.committed
            this.reject = true
            this.rejectHint = this@Raft.raftLog.lastIndex()
            this.to = this@Raft.leaderId
            this.requestSnapshot = this@Raft.pendingRequestSnapshot
            this@Raft.send(this)
        }
    }

    /// `pass_election_timeout` returns true iff `election_elapsed` is greater
    /// than or equal to the randomized election timeout in
    /// [`election_timeout`, 2 * `election_timeout` - 1].
    private fun passElectionTimeout(): Boolean = electionElapsed > randomizedElectionTimeout

    /// Stops the transfer of a leader.
    private fun abortLeaderTransfer() {
        leadTransferee = null
    }

    /// Returns a value representing the soft state at the time of calling.
    fun softState() = SoftState(leaderId = this.leaderId, raftState = this.state)

    /// Returns a value representing the hard state at the time of calling.
    fun hardState(): Eraftpb.HardState = Eraftpb.HardState.newBuilder().apply {
        this.commit = this@Raft.term
        this.vote = this@Raft.vote
        this.commit = this@Raft.term
    }.build()

    /// Checks if logs are committed to its term.
    ///
    /// The check is useful usually when raft is leader.
    private fun commitToCurrentTerm(): Boolean = try {
        this.raftLog.term(this.raftLog.committed) == this.term
    } catch (e: RaftErrorException) {
        false
    }

    /// Commit that the Raft peer has applied up to the given index.
    ///
    /// Registers the new applied index to the Raft log.
    ///
    /// # Hooks
    ///
    /// * Post: Checks to see if it's time to finalize a Joint Consensus state.
    fun commitApply(applied: Long) = this.raftLog.appliedTo(applied)

    /// Broadcasts heartbeats to all the followers if it's leader.
    fun ping() {
        if (this.state == StateRole.Leader) {
            this.bcastHeartbeat()
        }
    }

    /// # Errors
    ///
    /// * `id` is already a voter.
    /// * `id` is already a learner.
    fun addVoterOrLearner(id: Long, role: ProgressRole) {
        if (logger.isDebugEnabled) {
            logger.debug { "adding node ${role.name} with ID $id to peers." }
        }

        when {
            role == ProgressRole.LEARNER -> {
                val progress = Progress(this.raftLog.lastIndex() + 1, this.maxInflight)
                this.prs.insertVoterOrLearner(id, progress, role)
            }
            this.prs.learnerIds().contains(id) -> {
                this.prs.promoteLearner(id)
            }
            else -> {
                val progress = Progress(this.raftLog.lastIndex() + 1, this.maxInflight)
                this.prs.insertVoterOrLearner(id, progress, role)
            }
        }

        if (this.id == id) {
            this.promotable = role == ProgressRole.VOTER
        }

        // When a node is first added/promoted, we should mark it as recently active.
        // Otherwise, check_quorum may cause us to step down if it is invoked
        // before the added node has a chance to communicate with us.
        this.prs.progress[id]!!.recentActive = true
    }

    /// Removes a node from the raft.
    ///
    /// # Errors
    ///
    /// * `id` is not a voter or learner.
    fun removeNode(id: Long) {
        this.prs.remove(id)

        // do not try to commit or abort transferring if there are no voters in the cluster.
        if (this.prs.voterIds().isEmpty()) {
            return
        }

        // The quorum size is now smaller, so see if any pending entries can
        // be committed.
        if (this.maybeCommit()) {
            this.bcastAppend()
        }

        // The quorum size is now smaller, consider to response some read requests.
        // If there is only one peer, all pending read requests must be response.
        this.handleReadIndexAdvance(this.id, this.readOnly.lastPendingRequestCtx())

        // If the removed node is the lead_transferee, then abort the leadership transferring.
        if (this.state == StateRole.Leader) {
            this.leadTransferee?.run {
                if (this == id) {
                    this@Raft.abortLeaderTransfer()
                }
            }
        }
    }

    /// Grabs a reference to the snapshot
    fun snap(): Eraftpb.Snapshot? = this.raftLog.unstable.snapshot

    /// Request a snapshot from a leader.
    fun requestSnapshot(requestIdx: Long) {
        if (this.state == StateRole.Leader) {
            logger.info { "can not request snapshot on leader; dropping request snapshot" }
            raftError(RaftError.RequestSnapshotDropped)
        }

        if (this.leaderId == INVALID_ID) {
            logger.info { "drop request snapshot because of no leader" }
            raftError(RaftError.RequestSnapshotDropped)
        }

        if (this.snap() != null || this.pendingRequestSnapshot != INVALID_INDEX) {
            logger.info { "there is a pending snapshot; dropping request snapshot" }
            raftError(RaftError.RequestSnapshotDropped)
        }

        this.pendingRequestSnapshot = requestIdx
        this.sendRequestSnapshot()
        return
    }

    /// Set whether skip broadcast empty commit messages at runtime.
    fun skipBcastCommit(skip: Boolean) {
        this.skipBcastCommit = skip
    }

    /// Set whether batch append msg at runtime.
    fun batchAppend(batchAppend: Boolean) {
        this.batchAppend = batchAppend
    }

    // responseToReadIndexReq constructs a response for `req`. If `req` comes from the peer
    // itself, a blank value will be returned.
    private fun responseToReadIndexReq(req: Eraftpb.Message.Builder, readIndex: Long) {
        if (req.from == INVALID_ID || req.from == this@Raft.id) {
            // from local member
            val newRs = ReadState(
                index = readIndex,
                requestCtx = req.entriesBuilderList[0].data
            )
            this@Raft.readStates.add(newRs)
        } else {
            Eraftpb.Message.newBuilder().run {
                this.msgType = MsgReadIndexResp
                this.to = req.from
                this.index = index
                this.addAllEntries(req.entriesList)
                this@Raft.send(this)
            }
        }
    }

    // handle readIndex response
    private fun handleReadIndexAdvance(id: Long, ctx: ByteString?) {
        if (ctx == null || ctx.isEmpty) {
            return
        }

        val recvAck = this.readOnly.recvAck(id, ctx)
        if (recvAck == null || !prs.hasQuorum(recvAck)) {
            return
        }

        this.readOnly.advance(ctx, this.logger).forEach {
            this.responseToReadIndexReq(it.req, it.index)
        }
    }

}