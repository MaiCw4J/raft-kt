package com.mamba

import eraftpb.Eraftpb

/// Truncates the list of entries down to a specific byte-length of
/// all entries together.
///
/// # Examples
///
/// ```
/// use raft::{util::limit_size, prelude::*};
///
/// let template = {
///     let mut entry = Entry::default();
///     entry.data = "*".repeat(100).into_bytes();
///     entry
/// };
///
/// // Make a bunch of entries that are ~100 bytes long
/// let mut entries = vec![
///     template.clone(),
///     template.clone(),
///     template.clone(),
///     template.clone(),
///     template.clone(),
/// ];
///
/// assert_eq!(entries.len(), 5);
/// limit_size(&mut entries, Some(220));
/// assert_eq!(entries.len(), 2);
///
/// // `entries` will always have at least 1 Message
/// limit_size(&mut entries, Some(0));
/// assert_eq!(entries.len(), 1);
/// ```
fun limitSize(entries: Vec<Eraftpb.Entry>, max: Long?) {
    if (entries.size <= 1 || max == null) {
        return
    }

    var size = 0
    val limit = entries.takeWhile {
        if (size == 0) {
            size += it.serializedSize
            true
        } else {
            size += it.serializedSize
            size <= max
        }
    }.size

    entries.truncate(limit)
}

/// Check whether the entry is continuous to the message.
/// i.e msg's next entry index should be equal to the first entries's index
fun isContinuousEntries(msg: Eraftpb.Message.Builder, entries: Vec<Eraftpb.Entry>): Boolean {
    if (msg.entriesCount > 0 && entries.isNotEmpty()) {
        return msg.entriesList.last().index + 1 == entries.first().index
    }
    return true
}

fun majority(total: Int) = total / 2 + 1