package com.mamba.confchange

import com.mamba.Vec
import com.mamba.tracker.ProgressTracker
import com.mamba.vec
import eraftpb.Eraftpb

/// Creates a `ConfChangeSingle`.
fun newConfChangeSingle(nodeId: Long, cct: Eraftpb.ConfChangeType): Eraftpb.ConfChangeSingle {
    return Eraftpb.ConfChangeSingle.newBuilder().apply {
        this.nodeId = nodeId
        this.ccType = cct
    }.build()
}

/// Translates a conf state into 1) a slice of operations creating first the config that
/// will become the outgoing one, and then the incoming one, and b) another slice that,
/// when applied to the config resulted from 1), represents the ConfState.
fun toConfChangeSingle(cs: Eraftpb.ConfState): Pair<Vec<Eraftpb.ConfChangeSingle>, Vec<Eraftpb.ConfChangeSingle>> {
    // Example to follow along this code:
    // voters=(1 2 3) learners=(5) outgoing=(1 2 4 6) learners_next=(4)
    //
    // This means that before entering the joint config, the configuration
    // had voters (1 2 4 6) and perhaps some learners that are already gone.
    // The new set of voters is (1 2 3), i.e. (1 2) were kept around, and (4 6)
    // are no longer voters; however 4 is poised to become a learner upon leaving
    // the joint state.
    // We can't tell whether 5 was a learner before entering the joint config,
    // but it doesn't matter (we'll pretend that it wasn't).
    //
    // The code below will construct
    // outgoing = add 1; add 2; add 4; add 6
    // incoming = remove 1; remove 2; remove 4; remove 6
    //            add 1;    add 2;    add 3;
    //            add-learner 5;
    //            add-learner 4;
    //
    // So, when starting with an empty config, after applying 'outgoing' we have
    //
    //   quorum=(1 2 4 6)
    //
    // From which we enter a joint state via 'incoming'
    //
    //   quorum=(1 2 3)&&(1 2 4 6) learners=(5) learners_next=(4)
    //
    // as desired.
    val incoming = vec<Eraftpb.ConfChangeSingle>()
    val outgoing = vec<Eraftpb.ConfChangeSingle>()

    for (id in cs.votersOutgoingList) {
        // If there are outgoing voters, first add them one by one so that the
        // (non-joint) config has them all.
        outgoing.add(newConfChangeSingle(id, Eraftpb.ConfChangeType.AddNode))

        // We're done constructing the outgoing slice, now on to the incoming one
        // (which will apply on top of the config created by the outgoing slice).

        // First, we'll remove all of the outgoing voters.
        incoming.add(newConfChangeSingle(id, Eraftpb.ConfChangeType.RemoveNode))
    }

    // Then we'll add the incoming voters and learners.
    for (id in cs.votersList) {
        incoming.add(newConfChangeSingle(id, Eraftpb.ConfChangeType.AddNode))
    }
    for (id in cs.learnersList) {
        incoming.add(newConfChangeSingle(id, Eraftpb.ConfChangeType.AddLearnerNode))
    }

    // Same for LearnersNext; these are nodes we want to be learners but which
    // are currently voters in the outgoing config.
    for (id in cs.learnersNextList) {
        incoming.add(newConfChangeSingle(id, Eraftpb.ConfChangeType.AddLearnerNode))
    }

    return outgoing to incoming
}


/// Restore takes a Changer (which must represent an empty configuration), and runs a
/// sequence of changes enacting the configuration described in the ConfState.
fun restore(tracker: ProgressTracker, nextIdx: Long, cs: Eraftpb.ConfState) {
    val (outgoing, incoming) = toConfChangeSingle(cs)

    if (outgoing.isEmpty()) {
        for (cc in incoming) {
            val (cfg, changes) = Changer(tracker).simple(arrayOf(cc))
            tracker.applyConf(cfg, changes, nextIdx)
        }
        return
    }

    for (cc in outgoing) {
        val (cfg, changes) = Changer(tracker).simple(arrayOf(cc))
        tracker.applyConf(cfg, changes, nextIdx)
    }

    // Now enter the joint state, which rotates the above additions into the
    // outgoing config, and adds the incoming config in. Continuing the
    // example above, we'd get (1 2 3)&(2 3 4), i.e. the incoming operations
    // would be removing 2,3,4 and then adding in 1,2,3 while transitioning
    // into a joint state.
    val (cfg, changes) = Changer(tracker).enterJoint(cs.autoLeave, incoming.toTypedArray())
    tracker.applyConf(cfg, changes, nextIdx)
}
