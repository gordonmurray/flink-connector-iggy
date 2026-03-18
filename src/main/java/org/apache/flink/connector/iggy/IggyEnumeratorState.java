package org.apache.flink.connector.iggy;

import java.io.Serializable;
import java.util.List;

/**
 * Checkpoint state for the {@link IggySplitEnumerator}.
 * On restore, all splits are treated as unassigned until readers re-register.
 */
public record IggyEnumeratorState(
        List<IggySplit> assignedSplits,
        List<IggySplit> unassignedSplits
) implements Serializable {

    public IggyEnumeratorState {
        assignedSplits = List.copyOf(assignedSplits);
        unassignedSplits = List.copyOf(unassignedSplits);
    }
}
