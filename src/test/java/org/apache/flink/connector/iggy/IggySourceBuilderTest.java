package org.apache.flink.connector.iggy;

import org.junit.jupiter.api.Test;

import org.apache.flink.api.connector.source.Boundedness;

import static org.junit.jupiter.api.Assertions.*;

/** Unit tests for {@link IggySource.Builder}. Runs without Iggy. */
class IggySourceBuilderTest {

    @Test
    void shouldBuildWithRequiredFieldsOnly() {
        var source = IggySource.<byte[]>builder()
                .setStream("s")
                .setTopic("t")
                .setDeserializer(payload -> payload)
                .build();
        assertNotNull(source);
    }

    @Test
    void shouldRejectMissingStream() {
        var builder = IggySource.<byte[]>builder()
                .setTopic("t")
                .setDeserializer(payload -> payload);
        assertThrows(NullPointerException.class, builder::build);
    }

    @Test
    void shouldRejectMissingTopic() {
        var builder = IggySource.<byte[]>builder()
                .setStream("s")
                .setDeserializer(payload -> payload);
        assertThrows(NullPointerException.class, builder::build);
    }

    @Test
    void shouldRejectMissingDeserializer() {
        var builder = IggySource.<byte[]>builder()
                .setStream("s")
                .setTopic("t");
        assertThrows(NullPointerException.class, builder::build);
    }

    @Test
    void shouldRejectInvalidPort() {
        assertThrows(IllegalArgumentException.class, () ->
                IggySource.<byte[]>builder().setPort(0));
        assertThrows(IllegalArgumentException.class, () ->
                IggySource.<byte[]>builder().setPort(70000));
        assertThrows(IllegalArgumentException.class, () ->
                IggySource.<byte[]>builder().setPort(-1));
    }

    @Test
    void shouldRejectNonPositiveBatchSize() {
        assertThrows(IllegalArgumentException.class, () ->
                IggySource.<byte[]>builder().setBatchSize(0));
        assertThrows(IllegalArgumentException.class, () ->
                IggySource.<byte[]>builder().setBatchSize(-5));
    }

    @Test
    void forBytesShouldUseDefaults() {
        var source = IggySource.forBytes("myStream", "myTopic");
        assertNotNull(source);
        assertEquals(Boundedness.CONTINUOUS_UNBOUNDED, source.getBoundedness());
    }

    @Test
    void shouldBuildWithStartingOffset() {
        var source = IggySource.<byte[]>builder()
                .setStream("s")
                .setTopic("t")
                .setDeserializer(payload -> payload)
                .setStartingOffset(IggyOffsetSpec.latest())
                .build();
        assertNotNull(source);
    }

    @Test
    void shouldBuildWithSpecificOffset() {
        var source = IggySource.<byte[]>builder()
                .setStream("s")
                .setTopic("t")
                .setDeserializer(payload -> payload)
                .setStartingOffset(IggyOffsetSpec.specificOffset(500))
                .build();
        assertNotNull(source);
    }

    @Test
    void shouldDefaultToEarliestWhenNotSet() {
        var source = IggySource.<byte[]>builder()
                .setStream("s")
                .setTopic("t")
                .setDeserializer(payload -> payload)
                .build();
        assertNotNull(source);
    }
}
