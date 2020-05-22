package com.mamba.exception

import java.lang.Exception

class RaftErrorException: Exception {

    val error: RaftError

    constructor(error: RaftError): super(error.desc) {
        this.error = error
    }

    constructor(error: RaftError, message: String): super(message) {
        this.error = error
    }

}