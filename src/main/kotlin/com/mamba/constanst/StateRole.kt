package com.mamba.constanst

/**
 *
 * @author mcw
 * @date 2020/04/09 14:02
 *
 */
enum class StateRole {
    /// The node is a follower of the leader.
    Follower,

    /// The node could become a leader.
    Candidate,

    /// The node is a leader.
    Leader,

    /// The node could become a candidate, if `prevote` is enabled.
    PreCandidate,
}