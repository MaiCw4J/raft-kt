package com.mamba

import com.mamba.constanst.ProgressState
import com.mamba.tracker.Progress
import org.junit.Test
import kotlin.test.assertEquals

class RaftTest {

    @Test
    fun progressBecomeProbeTest() {
        val matched = 1L

        val testCase = listOf(
            (newProgress(ProgressState.Replicate, matched, 5, 0, 256) to 2L),
            // snapshot finish
            (newProgress(ProgressState.Snapshot, matched, 5, 10, 256) to 11L),
            // snapshot failure
            (newProgress(ProgressState.Snapshot, matched, 5, 0, 256) to 2L)
        )

        testCase.forEachIndexed { i, (p, n) ->
            p.becomeProbe()

            assertEquals(p.state, ProgressState.Probe, "$i state case failed")
            assertEquals(p.matched, matched, "$i match case failed")
            assertEquals(p.nextIdx, n, "$i next index case failed")
        }

    }

    @Test
    fun progressBecomeReplicateTest() {
        val progress = newProgress(ProgressState.Probe, 1, 5, 0, 256)

        progress.becomeReplicate()

        assertEquals(progress.state, ProgressState.Replicate)
        assertEquals(progress.matched, 1)
        assertEquals(progress.matched + 1, progress.nextIdx)
    }

    @Test
    fun progressBecomeSnapshotTest() {
        val progress = newProgress(ProgressState.Probe, 1, 5, 0, 256)

        val pendingSnapshot = 10L

        progress.becomeSnapshot(pendingSnapshot)

        assertEquals(progress.state, ProgressState.Snapshot)
        assertEquals(progress.matched, 1)
        assertEquals(progress.pendingSnapshot, pendingSnapshot)
    }

    private fun newProgress(state: ProgressState,
                            matched: Long,
                            next_idx: Long,
                            pending_snapshot: Long,
                            ins_size: Int): Progress {
        val progress = Progress(next_idx, ins_size)
        progress.state = state
        progress.matched = matched
        progress.pendingRequestSnapshot = pending_snapshot
        return progress
    }

}