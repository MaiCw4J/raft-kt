package com.mamba.tracker

import com.mamba.quorum.JointConfig
import eraftpb.Eraftpb

/// Config reflects the configuration tracked in a ProgressTracker.
class Configuration {
    val votes: JointConfig

    /// Learners is a set of IDs corresponding to the learners active in the
    /// current configuration.
    ///
    /// Invariant: Learners and Voters does not intersect, i.e. if a peer is in
    /// either half of the joint config, it can't be a learner; if it is a
    /// learner it can't be in either half of the joint config. This invariant
    /// simplifies the implementation since it allows peers to have clarity about
    /// its current role without taking into account joint consensus.
    val learners: MutableSet<Long>

    /// When we turn a voter into a learner during a joint consensus transition,
    /// we cannot add the learner directly when entering the joint state. This is
    /// because this would violate the invariant that the intersection of
    /// voters and learners is empty. For example, assume a Voter is removed and
    /// immediately re-added as a learner (or in other words, it is demoted):
    ///
    /// Initially, the configuration will be
    ///
    ///   voters:   {1 2 3}
    ///   learners: {}
    ///
    /// and we want to demote 3. Entering the joint configuration, we naively get
    ///
    ///   voters:   {1 2} & {1 2 3}
    ///   learners: {3}
    ///
    /// but this violates the invariant (3 is both voter and learner). Instead,
    /// we get
    ///
    ///   voters:   {1 2} & {1 2 3}
    ///   learners: {}
    ///   next_learners: {3}
    ///
    /// Where 3 is now still purely a voter, but we are remembering the intention
    /// to make it a learner upon transitioning into the final configuration:
    ///
    ///   voters:   {1 2}
    ///   learners: {3}
    ///   next_learners: {}
    ///
    /// Note that next_learners is not used while adding a learner that is not
    /// also a voter in the joint config. In this case, the learner is added
    /// right away when entering the joint configuration, so that it is caught up
    /// as soon as possible.
    val learnersNext: MutableSet<Long>

    /// True if the configuration is joint and a transition to the incoming
    /// configuration should be carried out automatically by Raft when this is
    /// possible. If false, the configuration will be joint until the application
    /// initiates the transition manually.
    val autoLeave: Boolean

    constructor(votes: MutableSet<Long>, learners: MutableSet<Long>) {
        this.votes = JointConfig(votes)
        this.learners = learners
        this.learnersNext = mutableSetOf()
        this.autoLeave = false
    }

    constructor(voters: Int, learners: Int) {
        this.votes = JointConfig(voters)
        this.learners = HashSet(learners)
        this.learnersNext = mutableSetOf()
        this.autoLeave = false
    }

    fun toConfState(): Eraftpb.ConfState = Eraftpb.ConfState.newBuilder().apply {
        this.addAllVoters(this@Configuration.votes.incoming.voters)
        this.addAllVotersOutgoing(this@Configuration.votes.outgoing.voters)
        this.addAllLearners(this@Configuration.learners)
        this.addAllLearnersNext(this@Configuration.learnersNext)
        this.autoLeave = this@Configuration.autoLeave
    }.build()

}