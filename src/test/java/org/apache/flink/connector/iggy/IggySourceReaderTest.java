package org.apache.flink.connector.iggy;

import org.apache.flink.api.connector.source.ReaderOutput;
import org.apache.flink.api.connector.source.SourceOutput;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.io.InputStatus;
import org.apache.flink.metrics.groups.SourceReaderMetricGroup;
import org.apache.flink.metrics.groups.UnregisteredMetricsGroup;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

@EnabledIfEnvironmentVariable(named = "IGGY_HOST", matches = ".+")
class IggySourceReaderTest {

    private static final String HOST = System.getenv().getOrDefault("IGGY_HOST", "localhost");
    private static final int PORT = Integer.parseInt(System.getenv().getOrDefault("IGGY_PORT", "8090"));

    static IggySourceReader<byte[]> createReader() {
        var config = new IggyConnectionConfig(HOST, PORT, "iggy", "iggy", false, null);
        return new IggySourceReader<>(new StubSourceReaderContext(), config, 100L, 5000L, 100, 1L, payload -> payload);
    }

    @Test
    void shouldPollMessagesAndAdvanceOffset() throws Exception {
        var reader = createReader();
        reader.start();
        reader.addSplits(Collections.singletonList(new IggySplit("crypto", "prices", 0, 0)));

        var collected = new ArrayList<byte[]>();
        var output = new CollectingReaderOutput(collected);

        assertEquals(InputStatus.MORE_AVAILABLE, reader.pollNext(output));
        assertFalse(collected.isEmpty());
        assertTrue(new String(collected.get(0)).startsWith("{"));

        var snapshot = reader.snapshotState(1L);
        assertEquals(1, snapshot.size());
        assertTrue(snapshot.get(0).getCurrentOffset() > 0);

        collected.clear();
        assertEquals(InputStatus.MORE_AVAILABLE, reader.pollNext(output));
        var snapshot2 = reader.snapshotState(2L);
        assertTrue(snapshot2.get(0).getCurrentOffset() > snapshot.get(0).getCurrentOffset());

        reader.close();
    }

    @Test
    void shouldReturnNothingAvailableWithNoSplits() throws Exception {
        var reader = createReader();
        reader.start();
        assertEquals(InputStatus.NOTHING_AVAILABLE, reader.pollNext(new CollectingReaderOutput(new ArrayList<>())));
        reader.close();
    }

    // -- Shared test helpers --

    static class CollectingReaderOutput implements ReaderOutput<byte[]> {
        private final List<byte[]> collected;
        CollectingReaderOutput(List<byte[]> collected) { this.collected = collected; }
        @Override public void collect(byte[] record) { collected.add(record); }
        @Override public void collect(byte[] record, long timestamp) { collected.add(record); }
        @Override public SourceOutput<byte[]> createOutputForSplit(String splitId) { return this; }
        @Override public void releaseOutputForSplit(String splitId) {}
        @Override public void markIdle() {}
        @Override public void markActive() {}
        @Override public void emitWatermark(org.apache.flink.api.common.eventtime.Watermark w) {}
    }

    static class StubSourceReaderContext implements SourceReaderContext {
        @Override public SourceReaderMetricGroup metricGroup() {
            return UnregisteredMetricsGroup.createSourceReaderMetricGroup();
        }
        @Override public Configuration getConfiguration() { return new Configuration(); }
        @Override public String getLocalHostName() { return "localhost"; }
        @Override public int getIndexOfSubtask() { return 0; }
        @Override public void sendSplitRequest() {}
        @Override public void sendSourceEventToCoordinator(
                org.apache.flink.api.connector.source.SourceEvent e) {}
        @Override public org.apache.flink.util.UserCodeClassLoader getUserCodeClassLoader() {
            var cl = Thread.currentThread().getContextClassLoader();
            return new org.apache.flink.util.UserCodeClassLoader() {
                @Override public ClassLoader asClassLoader() { return cl; }
                @Override public void registerReleaseHookIfAbsent(String n, Runnable r) {}
            };
        }
    }
}
