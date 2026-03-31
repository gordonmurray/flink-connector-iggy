package org.apache.flink.connector.iggy;

import org.apache.flink.table.factories.Factory;

import org.junit.jupiter.api.Test;

import java.util.ServiceLoader;

import static org.junit.jupiter.api.Assertions.*;

class IggyDynamicTableFactoryTest {

    @Test
    void shouldBeDiscoverableViaSpi() {
        var found = false;
        for (Factory f : ServiceLoader.load(Factory.class)) {
            if (f instanceof IggyDynamicTableFactory) { found = true; break; }
        }
        assertTrue(found, "IggyDynamicTableFactory should be discoverable via SPI");
    }

    @Test
    void shouldHaveCorrectIdentifier() {
        assertEquals("iggy", new IggyDynamicTableFactory().factoryIdentifier());
    }

    @Test
    void shouldRequireStreamAndTopic() {
        var factory = new IggyDynamicTableFactory();
        var keys = factory.requiredOptions().stream().map(o -> o.key()).toList();
        assertTrue(keys.contains("stream"));
        assertTrue(keys.contains("topic"));
    }

    @Test
    void shouldHaveDefaultsForConnectionOptions() {
        assertEquals("localhost", IggyDynamicTableFactory.HOST.defaultValue());
        assertEquals(8090, IggyDynamicTableFactory.PORT.defaultValue());
        assertEquals("iggy", IggyDynamicTableFactory.USERNAME.defaultValue());
        assertEquals("iggy", IggyDynamicTableFactory.PASSWORD.defaultValue());
        assertEquals(false, IggyDynamicTableFactory.TLS.defaultValue());
    }

    @Test
    void shouldHaveDefaultPollTimeout() {
        assertEquals(5000L, IggyDynamicTableFactory.POLL_TIMEOUT.defaultValue());
    }

    @Test
    void shouldExposePollTimeoutAsOptional() {
        var factory = new IggyDynamicTableFactory();
        var keys = factory.optionalOptions().stream().map(o -> o.key()).toList();
        assertTrue(keys.contains("poll.timeout"));
    }

    @Test
    void shouldExposeStartingOffsetAsOptional() {
        var factory = new IggyDynamicTableFactory();
        var keys = factory.optionalOptions().stream().map(o -> o.key()).toList();
        assertTrue(keys.contains("starting-offset"));
    }

    @Test
    void shouldHaveDefaultStartingOffset() {
        assertEquals("earliest", IggyDynamicTableFactory.STARTING_OFFSET.defaultValue());
    }
}
