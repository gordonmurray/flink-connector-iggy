package org.apache.flink.connector.iggy;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/** Unit tests for {@link IggyOffsetSpec} factory methods and validation. */
class IggyOffsetSpecTest {

    @Test
    void earliestShouldReturnEarliestPolicy() {
        var spec = IggyOffsetSpec.earliest();
        assertEquals(IggyOffsetPolicy.EARLIEST, spec.getPolicy());
        assertEquals(0L, spec.getSpecificOffset());
    }

    @Test
    void latestShouldReturnLatestPolicy() {
        var spec = IggyOffsetSpec.latest();
        assertEquals(IggyOffsetPolicy.LATEST, spec.getPolicy());
    }

    @Test
    void specificOffsetShouldStoreOffset() {
        var spec = IggyOffsetSpec.specificOffset(42L);
        assertEquals(IggyOffsetPolicy.SPECIFIC_OFFSET, spec.getPolicy());
        assertEquals(42L, spec.getSpecificOffset());
    }

    @Test
    void specificOffsetShouldAcceptZero() {
        var spec = IggyOffsetSpec.specificOffset(0L);
        assertEquals(0L, spec.getSpecificOffset());
    }

    @Test
    void specificOffsetShouldRejectNegative() {
        assertThrows(IllegalArgumentException.class, () -> IggyOffsetSpec.specificOffset(-1));
    }

    @Test
    void specificOffsetShouldRejectLargeNegative() {
        assertThrows(IllegalArgumentException.class, () -> IggyOffsetSpec.specificOffset(-100));
    }
}
