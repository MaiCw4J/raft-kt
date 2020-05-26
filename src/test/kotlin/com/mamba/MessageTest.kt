package com.mamba

import eraftpb.Eraftpb
import org.junit.Test
import kotlin.test.assertEquals

class MessageTest {

    @Test fun isLocalMessageTest() {
        val localMessage = listOf(
            Eraftpb.MessageType.MsgHup,
            Eraftpb.MessageType.MsgHeartbeat,
            Eraftpb.MessageType.MsgUnreachable,
            Eraftpb.MessageType.MsgSnapStatus,
            Eraftpb.MessageType.MsgCheckQuorum
        )
        for (value in Eraftpb.MessageType.values()) {
            assertEquals(value.isLocalMsg(), localMessage.contains(value))
        }
    }

    @Test fun isResponseMessageTest() {
        val localMessage = listOf(
            Eraftpb.MessageType.MsgAppendResponse,
            Eraftpb.MessageType.MsgRequestVoteResponse,
            Eraftpb.MessageType.MsgHeartbeatResponse,
            Eraftpb.MessageType.MsgUnreachable,
            Eraftpb.MessageType.MsgRequestPreVoteResponse
        )
        for (value in Eraftpb.MessageType.values()) {
            assertEquals(value.isResponseMsg(), localMessage.contains(value))
        }
    }

}
