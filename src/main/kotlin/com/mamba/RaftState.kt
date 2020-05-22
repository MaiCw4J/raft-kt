package com.mamba

import eraftpb.Eraftpb

class RaftState(val hardState: Eraftpb.HardState, val confState: Eraftpb.ConfState) {

    fun initialized(): Boolean = this.confState != Eraftpb.ConfState.getDefaultInstance()

}