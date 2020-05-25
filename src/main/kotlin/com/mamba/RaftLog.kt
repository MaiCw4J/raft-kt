package com.mamba

import com.mamba.exception.RaftError
import com.mamba.exception.RaftErrorException
import com.mamba.storage.Storage
import eraftpb.Eraftpb
import mu.KotlinLogging
import kotlin.math.max
import kotlin.math.min

/// Raft log implementation
class RaftLog<STORAGE : Storage> {
    /// Contains all stable entries since the last snapshot.
    val store: STORAGE

    /// Contains all unstable entries and snapshot.
    /// they will be saved into storage.
    val unstable: Unstable

    /// The highest log position that is known to be in stable storage
    /// on a quorum of nodes.
    var committed: Long

    /// The highest log position that the application has been instructed
    /// to apply to its state machine.
    ///
    /// Invariant: applied <= committed
    var applied: Long

    private val logger = KotlinLogging.logger {}

    constructor(storage: STORAGE) {
        val initIndex = storage.firstIndex() + 1

        this.store = storage
        this.committed = initIndex
        this.applied = initIndex
        this.unstable = Unstable(storage.lastIndex() + 1)
    }

    /// Grabs the term from the last entry.
    ///
    /// # Panics
    ///
    /// Panics if there are entries but the last term has been discarded.
    fun lastTerm(): Long = try {
        this.term(this.lastIndex())
    } catch (e: RaftErrorException) {
        panic("unexpected error when getting the last term", e)
    }

    /// For a given index, finds the term associated with it.
    @Throws(RaftErrorException::class)
    fun term(idx: Long): Long {
        // the valid term range is [index of dummy entry, last index]
        if (idx < this.firstIndex() - 1 || idx > this.lastIndex()) {
            return 0
        }
        return this.unstable.maybeTerm(idx) ?: try {
            this.store.term(idx)
        } catch (e: RaftErrorException) {
            when (e.error) {
                RaftError.Storage_Compacted, RaftError.Storage_Unavailable -> throw e
                else -> fatal(logger, "unexpected error: ${e.error.name}")
            }
        }
    }

    /// Returns th first index in the store that is available via entries
    ///
    /// # Panics
    ///
    /// Panics if the store doesn't have a first index.
    fun firstIndex(): Long = this.unstable.maybeFirstIndex() ?: this.store.firstIndex()

    /// Returns the last index in the store that is available via entries.
    ///
    /// # Panics
    ///
    /// Panics if the store doesn't have a last index.
    fun lastIndex(): Long = this.unstable.maybeLastIndex() ?: this.store.lastIndex()


    /// Finds the index of the conflict.
    ///
    /// It returns the first index of conflicting entries between the existing
    /// entries and the given entries, if there are any.
    ///
    /// If there are no conflicting entries, and the existing entries contain
    /// all the given entries, zero will be returned.
    ///
    /// If there are no conflicting entries, but the given entries contains new
    /// entries, the index of the first new entry will be returned.
    ///
    /// An entry is considered to be conflicting if it has the same index but
    /// a different term.
    ///
    /// The first entry MUST have an index equal to the argument 'from'.
    /// The index of the given entries MUST be continuously increasing.
    fun findConflict(entries: Array<Eraftpb.Entry.Builder>): Long {
        val lastIdx = this.lastIndex()
        for (e in entries) {
            if (!this.matchTerm(e.index, e.term)) {
                if (e.index <= lastIdx) {
                    logger.info { "found conflict at index ${e.index}" }
                }
                return e.index
            }
        }
        return 0
    }

    /// Answers the question: Does this index belong to this term?
    fun matchTerm(idx: Long, term: Long): Boolean = try {
        this.term(idx) == term
    } catch (e: RaftErrorException) {
        false
    }


    /// Returns None if the entries cannot be appended. Otherwise,
    /// it returns Some((conflict_index, last_index)).
    ///
    /// # Panics
    ///
    /// Panics if it finds a conflicting index less than committed index.
    fun maybeAppend(idx: Long, term: Long, committed: Long, entries: Array<Eraftpb.Entry.Builder>): Long? {
        if (this.matchTerm(idx, term)) {
            val conflictIdx = this.findConflict(entries)
            when {
                conflictIdx == 0L -> {
                }
                conflictIdx <= this.committed -> {
                    fatal(logger, "entry $conflictIdx conflict with committed entry ${this.committed}")
                }
                else -> {
                    val start = conflictIdx - (idx - 1)
                    this.append(entries.sliceArray(start.toInt()..entries.size))
                }
            }
            val lastNewIndex = idx + entries.size
            this.commitTo(min(committed, lastNewIndex))
            return lastNewIndex
        }
        return null
    }

    /// Sets the last committed value to the passed in value.
    ///
    /// # Panics
    ///
    /// Panics if the index goes past the last index.
    fun commitTo(commit: Long) {
        // never decrease commit
        if (this.committed >= commit) {
            return
        }
        val lastIdx = this.lastIndex()
        if (lastIdx < commit) {
            fatal(logger, "to_commit $commit is out of range [last_index $lastIdx]")
        }
        this.committed = commit
    }

    /// Appends a set of entries to the unstable list.
    fun append(entries: Array<Eraftpb.Entry.Builder>): Long {
        if (logger.isTraceEnabled) {
            logger.trace { "Entries being appended to unstable list" }
        }
        if (entries.isEmpty()) {
            return this.lastIndex()
        }
        val after = entries[0].index - 1
        if (after < this.committed) {
            fatal(logger, "after $after is out of range [committed ${this.committed}]")
        }

        this.unstable.truncateAndAppend(entries);
        return this.lastIndex()
    }

    /// Advance the applied index to the passed in value.
    ///
    /// # Panics
    ///
    /// Panics if the value passed in is not new or known.
    fun appliedTo(idx: Long) {
        if (idx == 0L) {
            return
        }
        if (this.committed < idx || idx < this.applied) {
            fatal(logger, "applied($idx) is out of range [prev_applied(${this.applied}), committed(${this.committed})")
        }
        this.applied = idx
    }

    private fun mustCheckOutofbounds(low: Long, high: Long): RaftError? {
        if (low > high) {
            fatal(logger, "invalid slice $low > $high")
        }
        val firstIndex = this.firstIndex()
        if (low < firstIndex) {
            return RaftError.Storage_Compacted
        }

        val length = this.lastIndex() + 1 - firstIndex
        if (low < firstIndex || high > firstIndex + length) {
            fatal(logger, "slice[$low,$high] out of bound[$firstIndex,${this.lastIndex()}]")
        }
        return null
    }

    /// Grabs a slice of entries from the raft. Unlike a rust slice pointer, these are
    /// returned by value. The result is truncated to the max_size in bytes.
    @Throws(RaftErrorException::class)
    fun slice(low: Long, high: Long, max: Long?): Vec<Eraftpb.Entry> {
        this.mustCheckOutofbounds(low, high)?.let {
            raftError(it)
        }

        val entries: Vec<Eraftpb.Entry> = vec()

        if (low == high) {
            return entries
        }

        if (low < this.unstable.offset) {
            val unstableHigh = min(high, this.unstable.offset)
            try {
                val storeEntries = this.store.entries(low, unstableHigh, max)
                entries.addAll(storeEntries)
                if (entries.size < unstableHigh - low) {
                    return entries
                }
            } catch (e: RaftErrorException) {
                when (e.error) {
                    RaftError.Storage_Compacted -> throw e
                    RaftError.Storage_Unavailable -> fatal(
                        logger,
                        "entries[$low:$unstableHigh] is unavailable from storage"
                    )
                    else -> fatal(logger, "unexpected error: ${e.error}")
                }
            }
        }

        val offset = this.unstable.offset
        if (high > offset) {
            val unstable = this.unstable.slice(max(low, offset), high).map { it.build() }
            entries.addAll(unstable)
        }

        limitSize(entries, max)

        return entries
    }

    /// Attempts to commit the index and term and returns whether it did.
    fun maybeCommit(maxIdx: Long, term: Long): Boolean {
        val r = maxIdx > this.committed && try {
            this.term(maxIdx)
        } catch (e: RaftErrorException) {
            0
        } == term
        if (r) {
            if (logger.isDebugEnabled) {
                logger.debug { "committing index $maxIdx" }
            }
            this.commitTo(maxIdx)
        }

        return r
    }

    /// Restores the current log from a snapshot.
    fun restore(snapshot: Eraftpb.Snapshot) {
        logger.info { "starts to restore snapshot [index: ${snapshot.metadata.index}, term: ${snapshot.metadata.term}]" }

        this.committed = snapshot.metadata.index
        this.unstable.restore(snapshot)
    }

    /// Determines if the given (lastIndex,term) log is more up-to-date
    /// by comparing the index and term of the last entry in the existing logs.
    /// If the logs have last entry with different terms, then the log with the
    /// later term is more up-to-date. If the logs end with the same term, then
    /// whichever log has the larger last_index is more up-to-date. If the logs are
    /// the same, the given log is up-to-date.
    fun isUpToDate(lastIdx: Long, term: Long): Boolean =
        term > this.lastTerm() || (term == this.lastTerm() && lastIdx >= this.lastIndex())

    /// Returns the current snapshot
    fun snapshot(requestIdx: Long): Eraftpb.Snapshot {
        val snapshot = this.unstable.snapshot
        if (snapshot != null) {
            if (snapshot.metadata.index > requestIdx) {
                return snapshot
            }
        }
        return this.store.snapshot(requestIdx)
    }

    /// Returns entries starting from a particular index and not exceeding a bytesize.
    fun entries(idx: Long, maxSize: Long?): Vec<Eraftpb.Entry> {
        val lastIdx = this.lastIndex()
        if (idx > lastIdx) {
            return vec()
        }
        return this.slice(idx, lastIdx + 1, maxSize)
    }

    /// Returns slice of entries that are not committed.
    fun unstableEntries(): Vec<Eraftpb.Entry.Builder>? {
        if (this.unstable.entries.isEmpty()) {
            return null
        }
        return this.unstable.entries
    }

    /// Returns any entries since the a particular index.
    fun nextEntriesSince(sinceIdx: Long): Vec<Eraftpb.Entry>? {
        val offset = max(sinceIdx + 1, this.firstIndex())
        val committed = this.committed + 1
        if (this.committed > offset) {
            try {
                return this.slice(offset, committed, null)
            } catch (e: RaftErrorException) {
                fatal(this.logger, e.error.desc)
            }
        }
        return null
    }

    /// Returns all the available entries for execution.
    /// If applied is smaller than the index of snapshot, it returns all committed
    /// entries after the index of snapshot.
    fun nextEntries(): Vec<Eraftpb.Entry>? = this.nextEntriesSince(this.applied)


    /// Attempts to set the stable up to a given index.
    fun stableTo(idx: Long, term: Long) = this.unstable.stableTo(idx, term)

    /// Snaps the unstable up to a current index.
    fun stableSnapTo(idx: Long) = this.unstable.stableSnapTo(idx)

    /// Returns whether there are entries that can be applied between `since_idx` and the comitted index.
    fun hasNextEntriesSince(sinceIdx: Long): Boolean = this.committed + 1 > max(sinceIdx + 1, this.firstIndex())

    /// Returns whether there are new entries.
    fun hasNextEntries(): Boolean = hasNextEntriesSince(this.applied)

}