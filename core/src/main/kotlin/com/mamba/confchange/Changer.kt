package com.mamba.confchange

import com.mamba.Vec
import com.mamba.exception.RaftError
import com.mamba.exception.RaftErrorException
import com.mamba.raftError
import com.mamba.tracker.Configuration
import com.mamba.tracker.ProgressMap
import com.mamba.tracker.ProgressTracker
import com.mamba.vec
import eraftpb.Eraftpb

/// Change log for progress map.
enum class MapChangeType {
    Add,
    Remove,
}

/// Changes made by `Changer`.
typealias MapChange = Vec<Pair<Long, MapChangeType>>

class IncrChangeMap(val changes: MapChange, val base: ProgressMap) {

    fun intoChanges(): MapChange = this.changes

    fun contains(id: Long): Boolean {
        return (this.changes.findLast { (i, _) -> i == id } ?: return this.base.containsKey(id)).second == MapChangeType.Add
    }
}

class Changer(private val tracker: ProgressTracker) {
    /// Verifies that the outgoing (=right) majority config of the joint
    /// config is empty and initializes it with a copy of the incoming (=left)
    /// majority config. That is, it transitions from
    /// ```text
    ///     (1 2 3)&&()
    /// ```
    /// to
    /// ```text
    ///     (1 2 3)&&(1 2 3)
    /// ```.
    ///
    /// The supplied changes are then applied to the incoming majority config,
    /// resulting in a joint configuration that in terms of the Raft thesis[1]
    /// (Section 4.3) corresponds to `C_{new,old}`.
    ///
    /// [1]: https://github.com/ongardie/dissertation/blob/master/online-trim.pdf
    @Throws(RaftErrorException::class)
    fun enterJoint(autoLeave: Boolean, ccs: Array<Eraftpb.ConfChangeSingle>): Pair<Configuration, MapChange> {
        if (this.joint(this.tracker.configuration)) {
            raftError(RaftError.ConfChangeError, "configuration is already joint")
        }

        val (prs, cfg) = checkAndCopy()

        if (cfg.votes.incoming.voters.isEmpty()) {
            // We allow adding nodes to an empty config for convenience (testing and
            // bootstrap), but you can't enter a joint state.
            raftError(RaftError.ConfChangeError, "can't make a zero-voter config joint")
        }

        cfg.votes.outgoing.voters.addAll(cfg.votes.incoming.voters)

        this.apply(cfg, prs, ccs)
        cfg.autoLeave = autoLeave

        checkInvariants(cfg, prs)

        return cfg to prs.intoChanges()
    }

    /// Transitions out of a joint configuration. It is an error to call this method if
    /// the configuration is not joint, i.e. if the outgoing majority config is empty.
    ///
    /// The outgoing majority config of the joint configuration will be removed, that is,
    /// the incoming config is promoted as the sole decision maker. In the notation of
    /// the Raft thesis[1] (Section 4.3), this method transitions from `C_{new,old}` into
    /// `C_new`.
    ///
    /// At the same time, any staged learners (LearnersNext) the addition of which was
    /// held back by an overlapping voter in the former outgoing config will be inserted
    /// into Learners.
    ///
    /// [1]: https://github.com/ongardie/dissertation/blob/master/online-trim.pdf
    @Throws(RaftErrorException::class)
    fun leaveJoint(): Pair<Configuration, MapChange> {
        if (!joint(this.tracker.configuration)) {
            raftError(RaftError.ConfChangeError, "can't leave a non-joint config")
        }
        val (prs, cfg) = checkAndCopy()

        cfg.learners.addAll(cfg.learnersNext)
        cfg.learnersNext.clear()

        for (id in cfg.votes.outgoing.voters) {
            if (!cfg.votes.incoming.voters.contains(id) && cfg.learners.contains(id)) {
                prs.changes.add(id to MapChangeType.Remove)
            }
        }

        cfg.votes.outgoing.voters.clear()
        cfg.autoLeave = false
        checkInvariants(cfg, prs)

        return cfg to prs.intoChanges()
    }

    /// Carries out a series of configuration changes that (in aggregate) mutates the
    /// incoming majority config Voters[0] by at most one. This method will return an
    /// error if that is not the case, if the resulting quorum is zero, or if the
    /// configuration is in a joint state (i.e. if there is an outgoing configuration).
    @Throws(RaftErrorException::class)
    fun simple(ccs: Array<Eraftpb.ConfChangeSingle>): Pair<Configuration, MapChange> {
        if (joint(this.tracker.configuration)) {
            raftError(RaftError.ConfChangeError, "can't apply simple config change in joint config")
        }

        val (prs, cfg) = checkAndCopy()

        this.apply(cfg, prs, ccs)

        if (cfg.votes.incoming.voters.subtract(this.tracker.configuration.votes.incoming.voters).size > 1) {
            raftError(RaftError.ConfChangeError, "more than one voter changed without entering joint config")
        }

        checkInvariants(cfg, prs)

        return cfg to prs.intoChanges()
    }

    private fun checkAndCopy(): Pair<IncrChangeMap, Configuration> {
        val prs = IncrChangeMap(
                changes = vec(),
                base = this.tracker.progress
        )

        val cfg = this.tracker.configuration

        checkInvariants(cfg, prs)

        return prs to cfg
    }

    /// Makes sure that the config and progress are compatible with each other.
    /// This is used to check both what the Changer is initialized with, as well
    /// as what it returns.
    private fun checkInvariants(cfg: Configuration, prs: IncrChangeMap) {
        // NB: intentionally allow the empty config. In production we'll never see a
        // non-empty config (we prevent it from being created) but we will need to
        // be able to *create* an initial config, for example during bootstrap (or
        // during tests). Instead of having to hand-code this, we allow
        // transitioning from an empty config into any other legal and non-empty
        // config.
        for (id in cfg.votes.ids()) {
            if (!prs.contains(id)) {
                raftError(RaftError.ConfChangeError, "no progress for voter $id")
            }
        }

        for (id in cfg.learners) {
            if (!prs.contains(id)) {
                raftError(RaftError.ConfChangeError, "no progress for learner $id")
            }

            // Conversely Learners and Voters doesn't intersect at all.
            if (cfg.votes.outgoing.voters.contains(id)) {
                raftError(RaftError.ConfChangeError, "$id is in learners and outgoing voters")
            }

            if (cfg.votes.incoming.voters.contains(id)) {
                raftError(RaftError.ConfChangeError, "$id is in learners and incoming voters")
            }
        }

        for (id in cfg.learnersNext) {
            if (!prs.contains(id)) {
                raftError(RaftError.ConfChangeError, "no progress for learner(next) $id")
            }

            // Any staged learner was staged because it could not be directly added due
            // to a conflicting voter in the outgoing config.
            if (!cfg.votes.outgoing.voters.contains(id)) {
                raftError(RaftError.ConfChangeError, "$id is in learners_next and outgoing voters")
            }
        }

        if (this.joint(cfg)) {
            // etcd enforces outgoing and learner_next to be nil map. But there is no nil
            // in rust. We just check empty for simplicity.
            if (cfg.learnersNext.isNotEmpty()) {
                raftError(RaftError.ConfChangeError, "learners_next must be empty when not joint")
            }
            if (cfg.autoLeave) {
                raftError(RaftError.ConfChangeError, "auto_leave must be false when not joint")
            }
        }
    }

    /// Applies a change to the configuration. By convention, changes to voters are always
    /// made to the incoming majority config. Outgoing is either empty or preserves the
    /// outgoing majority configuration while in a joint state.
    private fun apply(cfg: Configuration, prs: IncrChangeMap, ccs: Array<Eraftpb.ConfChangeSingle>) {
        for (cc in ccs) {
            if (cc.nodeId == 0L) {
                // Replaces the NodeID with zero if it decides (downstream of
                // raft) to not apply a change, so we have to have explicit code
                // here to ignore these.
                continue
            }
            when(cc.ccType) {
                Eraftpb.ConfChangeType.AddNode -> this.makeVoter(cfg, prs, cc.nodeId)
                Eraftpb.ConfChangeType.AddLearnerNode -> this.makeLearner(cfg, prs, cc.nodeId)
                Eraftpb.ConfChangeType.RemoveNode -> this.remove(cfg, prs, cc.nodeId)
                else -> { /* to do nothing */ }
            }
        }
        if (cfg.votes.incoming.voters.isEmpty()) {
            raftError(RaftError.ConfChangeError, "removed all voters")
        }
    }

    /// Adds or promotes the given ID to be a voter in the incoming majority config.
    private fun makeVoter(cfg: Configuration, prs: IncrChangeMap, id: Long) {
        if (prs.contains(id)) {
            this.initProgress(cfg, prs, id, false)
            return
        }
        cfg.votes.incoming.voters.add(id)
        cfg.learners.remove(id)
        cfg.learnersNext.remove(id)
    }

    /// Makes the given ID a learner or stages it to be a learner once an active joint
    /// configuration is exited.
    ///
    /// The former happens when the peer is not a part of the outgoing config, in which
    /// case we either add a new learner or demote a voter in the incoming config.
    ///
    /// The latter case occurs when the configuration is joint and the peer is a voter
    /// in the outgoing config. In that case, we do not want to add the peer as a learner
    /// because then we'd have to track a peer as a voter and learner simultaneously.
    /// Instead, we add the learner to LearnersNext, so that it will be added to Learners
    /// the moment the outgoing config is removed by LeaveJoint().
    private fun makeLearner(cfg: Configuration, prs: IncrChangeMap, id: Long) {
        if (!prs.contains(id)) {
            this.initProgress(cfg, prs, id, true)
            return
        }
        if (cfg.learners.contains(id)) {
            return
        }

        cfg.votes.incoming.voters.remove(id)
        cfg.learners.remove(id)
        cfg.learnersNext.remove(id)

        // Use LearnersNext if we can't add the learner to Learners directly, i.e.
        // if the peer is still tracked as a voter in the outgoing config. It will
        // be turned into a learner in LeaveJoint().
        //
        // Otherwise, add a regular learner right away.
        if (cfg.votes.outgoing.voters.contains(id)) {
            cfg.learnersNext.add(id)
        } else {
            cfg.learners.add(id)
        }
    }

    /// Removes this peer as a voter or learner from the incoming config.
    private fun remove(cfg: Configuration, prs: IncrChangeMap, id: Long) {
        if (!prs.contains(id)) {
            return
        }

        cfg.votes.incoming.voters.remove(id)
        cfg.learners.remove(id)
        cfg.learnersNext.remove(id)

        // If the peer is still a voter in the outgoing config, keep the Progress.
        if (!cfg.votes.outgoing.voters.contains(id)) {
            prs.changes.add(id to MapChangeType.Remove)
        }
    }

    /// Initializes a new progress for the given node or learner.
    private fun initProgress(cfg: Configuration, prs: IncrChangeMap, id: Long, isLearner: Boolean) {
        if (isLearner) {
            cfg.learners.add(id)
        } else {
            cfg.votes.incoming.voters.add(id)
        }
        prs.changes.add(id to MapChangeType.Add)
    }

    private fun joint(cfg: Configuration): Boolean = cfg.votes.outgoing.voters.isEmpty()

}