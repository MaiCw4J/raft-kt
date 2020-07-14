package com.mamba

import com.mamba.exception.PanicException
import com.mamba.exception.RaftError
import com.mamba.exception.RaftErrorException
import eraftpb.Eraftpb
import mu.KLogger
import java.util.*

typealias Vec<E> = LinkedList<E>

fun <E> vec(): Vec<E> {
    return LinkedList()
}

fun panic(message: String): Nothing {
    throw PanicException(message)
}

fun panic(message: String, cause: Throwable?): Nothing {
    throw PanicException(message, cause)
}

fun fatal(logger: KLogger, message: String): Nothing {
    logger.error { message }
    throw PanicException(message)
}

fun raftError(error: RaftError): Nothing {
    throw RaftErrorException(error)
}

fun raftError(error: RaftError, vararg args: Any?): Nothing {
    throw RaftErrorException(error, error.desc.format(args))
}

fun <E> Vec<E>.truncate(index: Int) {
    if (index > this.size) {
        return
    }
    for (i in index..this.size) {
        this.pollLast()
    }
}

fun <E> Vec<E>.drain(index: Int): Vec<E> {
    val drainResult = LinkedList<E>()

    if (index > this.size) {
        return drainResult
    }

    for (i in 0..index) {
        drainResult.add(this.pollFirst())
    }
    return drainResult
}

inline fun <reified E> Vec<E>.array(r: IntRange): Array<E> = this.subList(r.first, r.last).toTypedArray()

fun Eraftpb.MessageType.isLocalMsg(): Boolean = when (this) {
    Eraftpb.MessageType.MsgHup,
    Eraftpb.MessageType.MsgHeartbeat,
    Eraftpb.MessageType.MsgUnreachable,
    Eraftpb.MessageType.MsgSnapStatus,
    Eraftpb.MessageType.MsgCheckQuorum -> true
    else -> false
}

fun Eraftpb.MessageType.isResponseMsg(): Boolean = when (this) {
    Eraftpb.MessageType.MsgAppendResponse,
    Eraftpb.MessageType.MsgRequestVoteResponse,
    Eraftpb.MessageType.MsgHeartbeatResponse,
    Eraftpb.MessageType.MsgUnreachable,
    Eraftpb.MessageType.MsgRequestPreVoteResponse -> true
    else -> false
}
