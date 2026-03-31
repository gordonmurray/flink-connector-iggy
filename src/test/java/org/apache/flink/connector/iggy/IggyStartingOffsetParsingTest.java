package org.apache.flink.connector.iggy;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/** Unit tests for SQL starting-offset option parsing. */
class IggyStartingOffsetParsingTest {

    @Test
    void shouldParseEarliest() {
        var spec = IggyDynamicTableFactory.parseStartingOffset("earliest");
        assertEquals(IggyOffsetPolicy.EARLIEST, spec.getPolicy());
    }

    @Test
    void shouldParseLatest() {
        var spec = IggyDynamicTableFactory.parseStartingOffset("latest");
        assertEquals(IggyOffsetPolicy.LATEST, spec.getPolicy());
    }

    @Test
    void shouldParseSpecificOffset() {
        var spec = IggyDynamicTableFactory.parseStartingOffset("specific-offset:48291");
        assertEquals(IggyOffsetPolicy.SPECIFIC_OFFSET, spec.getPolicy());
        assertEquals(48291L, spec.getSpecificOffset());
    }

    @Test
    void shouldParseSpecificOffsetZero() {
        var spec = IggyDynamicTableFactory.parseStartingOffset("specific-offset:0");
        assertEquals(IggyOffsetPolicy.SPECIFIC_OFFSET, spec.getPolicy());
        assertEquals(0L, spec.getSpecificOffset());
    }

    @Test
    void shouldBeCaseInsensitive() {
        assertEquals(IggyOffsetPolicy.EARLIEST,
                IggyDynamicTableFactory.parseStartingOffset("EARLIEST").getPolicy());
        assertEquals(IggyOffsetPolicy.LATEST,
                IggyDynamicTableFactory.parseStartingOffset("Latest").getPolicy());
    }

    @Test
    void shouldDefaultToEarliestForNull() {
        var spec = IggyDynamicTableFactory.parseStartingOffset(null);
        assertEquals(IggyOffsetPolicy.EARLIEST, spec.getPolicy());
    }

    @Test
    void shouldDefaultToEarliestForBlank() {
        var spec = IggyDynamicTableFactory.parseStartingOffset("  ");
        assertEquals(IggyOffsetPolicy.EARLIEST, spec.getPolicy());
    }

    @Test
    void shouldRejectUnknownValue() {
        assertThrows(IllegalArgumentException.class,
                () -> IggyDynamicTableFactory.parseStartingOffset("unknown"));
    }

    @Test
    void shouldRejectInvalidSpecificOffset() {
        assertThrows(IllegalArgumentException.class,
                () -> IggyDynamicTableFactory.parseStartingOffset("specific-offset:abc"));
    }

    @Test
    void shouldRejectNegativeSpecificOffset() {
        assertThrows(IllegalArgumentException.class,
                () -> IggyDynamicTableFactory.parseStartingOffset("specific-offset:-1"));
    }
}
