package com.mamba.quorum

import kotlin.math.min

/// A configuration of two groups of (possibly overlapping) majority configurations.
/// Decisions require the support of both majorities.
class JointConfig {

    val incoming: MajorityConfig
    val outgoing: MajorityConfig

    /// Creates a new configuration using the given IDs.
    constructor(voters: MutableSet<Long>) {
        this.incoming = MajorityConfig(voters)
        this.outgoing = MajorityConfig(mutableSetOf())
    }

    /// Creates an empty configuration with given capacity.
    constructor(cap: Int) {
        this.incoming = MajorityConfig(cap)
        this.outgoing = MajorityConfig(0)
    }

    /// Returns the largest committed index for the given joint quorum. An index is
    /// jointly committed if it is committed in both constituent majorities.
    ///
    /// The bool flag indicates whether the index is computed by group commit algorithm
    /// successfully. It's true only when both majorities use group commit.
    fun committedIndex(f: AckIndex): Index = min(this.incoming.committedIndex(f), this.outgoing.committedIndex(f))

    /// Takes a mapping of voters to yes/no (true/false) votes and returns a result
    /// indicating whether the vote is pending, lost, or won. A joint quorum requires
    /// both majority quorums to vote in favor.
    fun voteResult(check: VoteCheck): VoteResult {
        val i = this.incoming.voteResult(check)
        val o = this.outgoing.voteResult(check)

        // It won if won in both.
        if (i == VoteResult.Won && o == VoteResult.Won) {
            return VoteResult.Won
        }
        // It lost if lost in either.
        if (i == VoteResult.Lost || o == VoteResult.Lost) {
            return VoteResult.Lost
        }
        // It remains pending if pending in both or just won in one side.
        return VoteResult.Pending
    }

    /// Clears all IDs.
    fun clear() {
        this.incoming.clear()
        this.outgoing.clear()
    }

    /// Returns true if (and only if) there is only one voting member
    /// (i.e. the leader) in the current configuration.
    fun isSingleton(): Boolean = this.outgoing.voters.isEmpty() && this.incoming.voters.size == 1

    /// Check if an id is a voter.
    fun contains(id: Long): Boolean = this.incoming.voters.contains(id) || this.outgoing.voters.contains(id)

    /// Returns an iterator over two hash set without cloning.
    fun ids(): Set<Long> = this.incoming.voters.union(this.outgoing.voters)

}