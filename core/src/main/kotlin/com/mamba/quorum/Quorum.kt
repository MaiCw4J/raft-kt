package com.mamba.quorum

import com.mamba.majority

typealias Index = Long
typealias AckIndex = (Long) -> Index?
typealias VoteCheck = (Long) -> Boolean?

/// A set of IDs that uses majority quorums to make decisions.
class MajorityConfig {

    val voters: MutableSet<Long>

    constructor(voters: MutableSet<Long>) {
        this.voters = voters
    }

    constructor(cap: Int) {
        this.voters = HashSet(cap)
    }

    /// Computes the committed index from those supplied via the
    /// provided AckIndexer (for the active config).
    ///
    /// The bool flag indicates whether the index is computed by group commit algorithm
    /// successfully.
    ///
    /// Eg. If the matched indexes are [2,2,2,4,5], it will return 2.
    fun committedIndex(f: AckIndex): Index {
        if (this.voters.isEmpty()) {
            // This plays well with joint quorums which, when one half is the zero
            // MajorityConfig, should behave like the other half.
            return Long.MAX_VALUE
        }

        val matched = this.voters.map { s -> f(s) ?: 0 }

        // Reverse sort.
        matched.sorted()

        // The smallest index into the array for which the value is acked by a
        // quorum. In other words, from the end of the slice, move n/2+1 to the
        // left (accounting for zero-indexing).
        return matched[majority(matched.size)]
    }

    /// Takes a mapping of voters to yes/no (true/false) votes and returns
    /// a result indicating whether the vote is pending (i.e. neither a quorum of
    /// yes/no has been reached), won (a quorum of yes has been reached), or lost (a
    /// quorum of no has been reached).
    fun voteResult(check: VoteCheck): VoteResult {
        if (this.voters.isEmpty()) {
            // By convention, the elections on an empty config win. This comes in
            // handy with joint quorums because it'll make a half-populated joint
            // quorum behave like a majority quorum.
            return VoteResult.Won
        }

        var yes = 0; var missing = 0
        for (voter in this.voters) {
            when(check(voter)) {
                null -> missing++
                true -> yes++
                else -> {}
            }
        }

        val q = majority(this.voters.size)
        if (yes >= q) {
            return VoteResult.Won
        }

        if (yes + missing >= q) {
            return VoteResult.Pending
        }

        return VoteResult.Lost
    }

    /// Clears all IDs.
    fun clear() {
        this.voters.clear()
    }

}