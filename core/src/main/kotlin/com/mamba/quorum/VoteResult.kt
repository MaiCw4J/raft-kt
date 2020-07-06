package com.mamba.quorum

/// VoteResult indicates the outcome of a vote.
enum class VoteResult {
    /// Pending indicates that the decision of the vote depends on future
    /// votes, i.e. neither "yes" or "no" has reached quorum yet.
    Pending,
    // Lost indicates that the quorum has voted "no".
    Lost,
    // Won indicates that the quorum has voted "yes".
    Won,
}