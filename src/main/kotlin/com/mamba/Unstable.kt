package com.mamba

import eraftpb.Eraftpb
import mu.KotlinLogging

class Unstable {
    /// The incoming unstable snapshot, if any.
    var snapshot: Eraftpb.Snapshot?

    /// All entries that have not yet been written to storage.
    val entries: Vec<Eraftpb.Entry.Builder>

    /// The offset from the vector index.
    var offset: Long

    private val logger = KotlinLogging.logger {}

    constructor(offset: Long) {
        this.snapshot = null
        this.entries = vec()
        this.offset = offset
    }

    /// Returns the index of the first possible entry in entries
    /// if it has a snapshot.
    fun maybeFirstIndex(): Long? = snapshot?.metadata?.index?.plus(1)

    /// Returns the last index if it has at least one unstable entry or snapshot.
    fun maybeLastIndex(): Long? = if (entries.isEmpty()) {
        snapshot?.metadata?.index
    } else {
        offset + entries.size - 1
    }

    /// Returns the term of the entry at index idx, if there is any.
    fun maybeTerm(idx: Long): Long? {
        return if (idx < offset) {
            snapshot?.metadata?.takeIf { idx == it.index }?.term
        } else {
            this.maybeLastIndex()?.takeIf { idx <= it }?.let { entries[(idx - offset).toInt()].term }
        }
    }

    /// Moves the stable offset up to the index. Provided that the index
    /// is in the same election term.
    fun stableTo(idx: Long, term: Long) {
        maybeTerm(idx).let {
            if (it == term && idx > offset) {
                val start = idx + 1 - offset
                this.entries.drain(start.toInt())
                offset = idx + 1
            }
        }
    }


    /// Removes the snapshot from self if the index of the snapshot matches
    fun stableSnapTo(idx: Long) {
        snapshot?.metadata?.index.let {
            if (idx == it) {
                snapshot = null
            }
        }
    }

    /// From a given snapshot, restores the snapshot to self, but doesn't unpack.
    fun restore(snap: Eraftpb.Snapshot) {
        entries.clear()
        offset = snap.metadata.index
        snapshot = snap
    }

    /// Append entries to unstable, truncate local block first if overlapped.
    fun truncateAndAppend(entries: Array<Eraftpb.Entry.Builder>) {
        val after = entries[0].index
        when {
            after == this.offset + this.entries.size -> {
                // after is the next index in the self.entries, append directly
                this.entries.addAll(entries)
            }
            after <= this.offset -> {
                // The log is being truncated to before our current offset
                // portion, so set the offset and replace the entries
                this.offset = after
                this.entries.clear()
                this.entries.addAll(entries)
            }
            else -> {
                // truncate to after and copy to self.entries then append
                val offset = this.offset
                this.mustCheckOutofbounds(offset, after)
                this.entries.truncate((after - offset).toInt())
                this.entries.addAll(entries)
            }
        }
    }

    /// Asserts the `hi` and `lo` values against each other and against the
    /// entries themselves.
    private fun mustCheckOutofbounds(low: Long, high: Long) {
        if (low > high) {
            fatal(logger, "invalid unstable.slice $low > $high")
        }

        val upper = this.offset + this.entries.size
        if (low < this.offset || high > upper) {
            fatal(logger, "unstable.slice[$low, $high] out of bound[${this.offset}, $upper]")
        }
    }

    fun slice(low: Long, high: Long): Array<Eraftpb.Entry.Builder> {
        this.mustCheckOutofbounds(low, high)
        val l = low.toInt()
        val h = high.toInt()
        val offset = this.offset.toInt()
        return this.entries.array(l - offset..h - offset)
    }

}