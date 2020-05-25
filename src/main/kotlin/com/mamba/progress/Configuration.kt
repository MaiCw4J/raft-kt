package com.mamba.progress

import com.mamba.constanst.ProgressRole
import com.mamba.exception.RaftError
import com.mamba.majority
import com.mamba.raftError
import eraftpb.Eraftpb


class Configuration {
    /// The voter set.
    val votes: MutableSet<Long>

    /// The learner set.
    val learners: MutableSet<Long>

    constructor(votes: MutableSet<Long>, learners: MutableSet<Long>) {
        this.votes = votes
        this.learners = learners
    }

    constructor(voters: Int, learners: Int) {
        this.votes = HashSet(voters)
        this.learners = HashSet(learners)
    }

    constructor(confState: Eraftpb.ConfState) {
        this.votes = confState.votersList.toHashSet()
        this.learners = confState.learnersList.toHashSet()
    }

    fun toConfState(): Eraftpb.ConfState = Eraftpb.ConfState.newBuilder().apply {
        this.addAllVoters(this@Configuration.votes)
        this.addAllLearners(this@Configuration.learners)
    }.build()

    /// Validates that the configuration is not problematic.
    ///
    /// Namely:
    /// * There can be no overlap of voters and learners.
    /// * There must be at least one voter.
    fun valid() {
        if (this.votes.isEmpty()) {
            raftError(RaftError.ConfigInvalid, "There must be at least one voter.")
        }
        val intersect = (this.votes intersect this.learners).iterator()
        if (intersect.hasNext()) {
            raftError(RaftError.Exists, intersect.next(), ProgressRole.LEARNER.name)
        }
    }

    fun hasQuorum(potentialQuorum: Set<Long>): Boolean =
        (this.votes intersect potentialQuorum).size >= majority(this.votes.size)

    /// Returns whether or not the given `id` is a member of this configuration.
    fun contains(id: Long): Boolean = this.votes.contains(id) || this.learners.contains(id)

}