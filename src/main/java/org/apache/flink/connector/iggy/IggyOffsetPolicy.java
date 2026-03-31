package org.apache.flink.connector.iggy;

/**
 * Supported starting offset strategies for new splits.
 *
 * <p>Controls where a new Flink job (or a job restored from a savepoint with
 * explicit offset seek) begins consuming. Restored splits from checkpoints
 * always use their persisted offsets regardless of this setting.
 */
public enum IggyOffsetPolicy {

    /** Start from the first available message in the partition. */
    EARLIEST,

    /** Start from the next message to be written (skip all existing messages). */
    LATEST,

    /**
     * Start from a specific offset value.
     * Requires {@link IggyOffsetSpec#specificOffset(long)} to be set.
     */
    SPECIFIC_OFFSET
}
