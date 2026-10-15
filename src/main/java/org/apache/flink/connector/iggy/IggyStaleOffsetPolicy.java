package org.apache.flink.connector.iggy;

/**
 * What a reader does when a split's offset is beyond the end of its partition.
 *
 * <p>That only happens when the topic was deleted and recreated, or purged, after
 * the offset was checkpointed: Iggy restarts the partition at offset 0, and a poll
 * past the end returns an empty batch rather than an error. Without detection the
 * reader waits silently until the new partition grows past the old offset, then
 * skips every message below it (issue #3).
 */
public enum IggyStaleOffsetPolicy {
    /** Re-read the partition from offset 0: everything in a recreated topic is new. The default. */
    EARLIEST,
    /** Skip to the current end of the partition, dropping what is already there. */
    LATEST,
    /** Fail the task, so an operator decides. Restarts will fail again until the job is resubmitted. */
    FAIL
}
