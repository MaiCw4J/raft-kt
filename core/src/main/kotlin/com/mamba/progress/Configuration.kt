package com.mamba.progress

import com.mamba.quorum.MajorityConfiguration
import eraftpb.Eraftpb

/// Config reflects the configuration tracked in a ProgressTracker.
class Configuration {
    val votes: MajorityConfiguration

    /// Learners is a set of IDs corresponding to the learners active in the
    /// current configuration.
    ///
    /// Invariant: Learners and Voters does not intersect, i.e. if a peer is in
    /// either half of the joint config, it can't be a learner; if it is a
    /// learner it can't be in either half of the joint config. This invariant
    /// simplifies the implementation since it allows peers to have clarity about
    /// its current role without taking into account joint consensus.
    val learners: MutableSet<Long>

    constructor(votes: MutableSet<Long>, learners: MutableSet<Long>) {
        this.votes = MajorityConfiguration(votes)
        this.learners = learners
    }

    constructor(voters: Int, learners: Int) {
        this.votes = MajorityConfiguration(voters)
        this.learners = HashSet(learners)
    }

    constructor(confState: Eraftpb.ConfState) {
        this.votes = MajorityConfiguration(confState.votersList.toHashSet())
        this.learners = confState.learnersList.toHashSet()
    }

    fun toConfState(): Eraftpb.ConfState = Eraftpb.ConfState.newBuilder().apply {
        this.addAllVoters(this@Configuration.votes.voters)
        this.addAllLearners(this@Configuration.learners)
    }.build()

}