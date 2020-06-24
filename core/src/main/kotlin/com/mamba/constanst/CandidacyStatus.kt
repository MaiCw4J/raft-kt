package com.mamba.constanst

/// The status of an election according to a Candidate node.
///
/// This is returned by `progress_set.election_status(vote_map)`
enum class CandidacyStatus {
    /// The election has been won by this Raft.
    Elected,

    /// It is still possible to win the election.
    Eligible,

    /// It is no longer possible to win the election.
    Ineligible,
}