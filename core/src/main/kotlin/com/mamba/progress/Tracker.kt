package com.mamba.progress

import com.mamba.constanst.CandidacyStatus
import com.mamba.constanst.ProgressRole
import com.mamba.exception.RaftError
import com.mamba.majority
import com.mamba.raftError
import eraftpb.Eraftpb
import mu.KotlinLogging

/// `Tracker` contains several `Progress`es,
/// which could be `Leader`, `Follower` and `Learner`.
class Tracker {
    val progress: MutableMap<Long, Progress>

    /// The current configuration state of the cluster.
    val configuration: Configuration

    private val logger = KotlinLogging.logger {}

    /// Create a progress set with the specified sizes already reserved.
    constructor(voters: Int, learners: Int) {
        this.progress = HashMap(voters + learners)
        this.configuration = Configuration(voters, learners)
    }

    /// Returns the Candidate's eligibility in the current election.
    ///
    /// If it is still eligible, it should continue polling nodes and checking.
    /// Eventually, the election will result in this returning either `Elected`
    /// or `Ineligible`, meaning the election can be concluded.
    fun candidacyStatus(votes: Map<Long, Boolean>): CandidacyStatus {
        val accepts = mutableSetOf<Long>()
        val rejects = mutableSetOf<Long>()

        for (entry in votes.entries) {
            if (entry.value) {
                accepts.add(entry.key)
            } else {
                rejects.add(entry.key)
            }
        }

        return when {
            this.configuration.hasQuorum(accepts) -> CandidacyStatus.Elected
            this.configuration.hasQuorum(rejects) -> CandidacyStatus.Ineligible
            else -> CandidacyStatus.Eligible
        }
    }

    /// Returns the maximal committed index for the cluster.
    ///
    /// Eg. If the matched indexes are [2,2,2,4,5], it will return 2.
    fun maximalCommittedIndex(): Long {
        val matched = with(this.configuration.votes) {
            val matched = LongArray(this.size)

            for ((i, id) in this.withIndex()) {
                matched[i] = this@Tracker.progress[id]!!.matched
            }

            // Reverse sort.
            matched.sortDescending()

            matched
        }
        return matched[majority(matched.size)]
    }

    /// Returns the ids of all known voters.
    ///
    /// **Note:** Do not use this for majority/quorum calculation. The Raft node may be
    /// transitioning to a new configuration and have two qourums. Use `has_quorum` instead.
    fun voterIds(): Set<Long> = this.configuration.votes

    /// Returns the ids of all known learners.
    ///
    /// **Note:** Do not use this for majority/quorum calculation. The Raft node may be
    /// transitioning to a new configuration and have two qourums. Use `has_quorum` instead.
    fun learnerIds(): Set<Long> = this.configuration.learners

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
            if (id == perspective || pr.recentActive) {
                active.add(id)
            }
            pr.recentActive = false
        }
        return this.configuration.hasQuorum(active)
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
            this.configuration.votes.add(id)
        }

        for (id in metadata.confState.learnersList) {
            this.progress[id] = Progress(nextIdx, maxInflight)
            this.configuration.learners.add(id)
        }
    }

    /// Determine if a quorum is formed from the given set of nodes.
    ///
    /// This is the only correct way to verify you have reached a quorum for the whole group.
    fun hasQuorum(potentialQuorum: Set<Long>): Boolean = this.configuration.hasQuorum(potentialQuorum)

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
            ProgressRole.VOTER -> this.configuration.votes
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

        this.configuration.votes.remove(id)
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

        if (!this.configuration.votes.add(id)) {
            // Already existed, the caller should know this was a noop.
            raftError(RaftError.Exists, id, ProgressRole.VOTER.name)
        }
    }

}