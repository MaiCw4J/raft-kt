package com.mamba.tracker

import com.mamba.constanst.ProgressRole
import com.mamba.exception.RaftError
import com.mamba.quorum.VoteResult
import com.mamba.raftError
import eraftpb.Eraftpb
import mu.KotlinLogging

/// `Tracker` contains several `Progress`es,
/// which could be `Leader`, `Follower` and `Learner`.
class ProgressTracker {
    val progress: MutableMap<Long, Progress>

    private val votes: MutableMap<Long, Boolean>

    /// The current configuration state of the cluster.
    val configuration: Configuration

    private val logger = KotlinLogging.logger {}

    /// Create a tracker set with the specified sizes already reserved.
    constructor(voters: Int, learners: Int) {
        this.progress = HashMap(voters + learners)
        this.votes = HashMap(voters)
        this.configuration = Configuration(voters, learners)
    }

    /// Returns the maximal committed index for the cluster.
    ///
    /// Eg. If the matched indexes are [2,2,2,4,5], it will return 2.
    fun maximalCommittedIndex(): Long  = this.configuration.votes.committedIndex { id -> this.progress[id]?.matched }

    /// Returns the ids of all known voters.
    ///
    /// **Note:** Do not use this for majority/quorum calculation. The Raft node may be
    /// transitioning to a new configuration and have two quorum. Use `has_quorum` instead.
    fun voterIds(): Set<Long> = this.configuration.votes.ids()

    /// Returns the ids of all known learners.
    ///
    /// **Note:** Do not use this for majority/quorum calculation. The Raft node may be
    /// transitioning to a new configuration and have two qourums. Use `has_quorum` instead.
    fun learnerIds(): Set<Long> = this.configuration.learners.union(this.configuration.learnersNext)

    /// Returns the status of voters.
    ///
    /// **Note:** Do not use this for majority/quorum calculation. The Raft node may be
    /// transitioning to a new configuration and have two qourums. Use `has_quorum` instead.
    fun voters(): Map<Long, Progress> = this.progress.filter { this.voterIds().contains(it.key) }

    /// Returns the status of learners.
    ///
    /// **Note:** Do not use this for majority/quorum calculation. The Raft node may be
    /// transitioning to a new configuration and have two qourums. Use `has_quorum` instead.
    fun learners(): Map<Long, Progress> = this.progress.filter { this.learnerIds().contains(it.key) }

    /// Determines if the current quorum is active according to the this raft node.
    /// Doing this will set the `recent_active` of each peer to false.
    ///
    /// This should only be called by the leader.
    fun quorumRecentlyActive(perspective: Long): Boolean {
        val active = hashSetOf<Long>()
        for ((id, pr) in this.voters()) {
            if (id == perspective) {
                pr.recentActive = true
                active.add(id)
            } else if (pr.recentActive) {
                pr.recentActive = false
                active.add(id)
            }
        }
        return this.hasQuorum(active)
    }

    private fun clear() {
        this.progress.clear()
        this.configuration.votes.clear()
        this.configuration.learners.clear()
    }

    fun restoreSnapshotMeta(metadata: Eraftpb.SnapshotMetadata, nextIdx: Long, maxInflight: Int) {
        this.clear()

        for (id in metadata.confState.votersList) {
            this.progress[id] = Progress(nextIdx, maxInflight)
            this.configuration.votes.incoming.voters.add(id)
        }

        for (id in metadata.confState.learnersList) {
            this.progress[id] = Progress(nextIdx, maxInflight)
            this.configuration.learners.add(id)
        }
    }

    /// Determine if a quorum is formed from the given set of nodes.
    ///
    /// This is the only correct way to verify you have reached a quorum for the whole group.
    fun hasQuorum(potentialQuorum: Set<Long>): Boolean {
        return this.configuration.votes.voteResult { id ->
            if (potentialQuorum.contains(id)) {
                true
            } else {
                null
            }
        } == VoteResult.Won
    }

    /// Determine itself
    fun hasQuorum(id: Long): Boolean = hasQuorum(setOf(id))

    /// Adds a voter or learner to the group.
    ///
    /// # Errors
    ///
    /// * `id` is in the voter set.
    /// * `id` is in the learner set.
    fun insertVoterOrLearner(id: Long, pr: Progress, role: ProgressRole) {
        if (logger.isDebugEnabled) {
            logger.debug("inserting ${role.name} with id $id")
        }

        if (this.learnerIds().contains(id)) {
            raftError(RaftError.Exists, id, ProgressRole.LEARNER.name)
        }

        if (this.voterIds().contains(id)) {
            raftError(RaftError.Exists, id, ProgressRole.VOTER.name)
        }

        val collection = when (role) {
            ProgressRole.VOTER -> this.configuration.votes.incoming.voters
            ProgressRole.LEARNER -> this.configuration.learners
        }

        collection.add(id)

        this.progress[id] = pr
    }

    /// Removes the peer from the set of voters or learners.
    ///
    /// # Errors
    ///
    fun remove(id: Long): Progress? {
        if (logger.isDebugEnabled) {
            logger.debug { "Removing peer with id = $id" }
        }

        this.configuration.votes.incoming.voters.remove(id)
        this.configuration.learners.remove(id)

        return this.progress.remove(id)
    }

    /// Promote a learner to a peer.
    fun promoteLearner(id: Long) {
        if (logger.isDebugEnabled) {
            logger.debug { "Promoting peer with id $id" }
        }

        if (!this.configuration.learners.remove(id)) {
            // Wasn't already a learner. We can't promote what doesn't exist.
            raftError(RaftError.NotExists, id, ProgressRole.LEARNER.name)
        }

        if (!this.configuration.votes.incoming.voters.add(id)) {
            // Already existed, the caller should know this was a noop.
            raftError(RaftError.Exists, id, ProgressRole.VOTER.name)
        }
    }

    /// Returns the Candidate's eligibility in the current election.
    ///
    /// If it is still eligible, it should continue polling nodes and checking.
    /// Eventually, the election will result in this returning either `Elected`
    /// or `Ineligible`, meaning the election can be concluded.
    fun voteResult(): VoteResult = this.configuration.votes.voteResult { id -> this.votes[id] }

    /// Prepares for a new round of vote counting via recordVote.
    fun resetVotes() {
        this.votes.clear()
    }

    /// Records that the node with the given id voted for this Raft
    /// instance if v == true (and declined it otherwise).
    fun recordVote(id: Long, vote: Boolean) {
        this.votes[id] = vote
    }

    /// Returns true if (and only if) there is only one voting member
    /// (i.e. the leader) in the current configuration.
    fun isSingleton(): Boolean = this.configuration.votes.isSingleton()

}