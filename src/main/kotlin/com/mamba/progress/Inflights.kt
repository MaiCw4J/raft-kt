package com.mamba.progress

import com.mamba.panic

class Inflights {
    // the starting index in the buffer
    var start: Int
    // number of inflights in the buffer
    var count: Int
    // cap ring buffer
    val cap: Int
    // ring buffer
    val buffer: MutableList<Long>

    constructor(cap: Int) {
        this.start = 0
        this.count = 0
        this.cap = cap
        this.buffer = ArrayList(cap)
    }

    fun full(): Boolean = this.count == this.cap

    /// Adds an inflight into inflights
    fun add(inflight: Long) {
        if (this.full()) {
            panic("cannot add into a full inflights")
        }
        var next = this.start + this.count
        if (next >= this.cap) {
            next -= this.cap
        }
        assert(next <= this.buffer.size)
        if (next == this.buffer.size) {
            this.buffer.add(inflight)
        } else {
            this.buffer[next] = inflight
        }
        this.count++
    }

    /// Frees the inflights smaller or equal to the given `to` flight.
    fun freeTo(to: Long) {
        if (this.count == 0 || to < this.buffer[this.start]) {
            // out of the left side of the window
            return
        }

        var i = 0
        var idx = this.start
        while (i < this.count) {
            if (to < this.buffer[idx]) {
                // found the first large inflight
                break
            }

            if (++idx >= this.cap) {
                idx -= this.cap
            }
            i++
        }
        // free i inflights and set new start index
        this.count -= i
        this.start = idx
    }

    /// Frees the first buffer entry.
    fun freeFirstOne() {
        val start = this.buffer[this.start]
        this.freeTo(start)
    }

    /// Frees all inflights.
    fun reset() {
        this.count = 0
        this.start = 0
    }

}