package org.apache.flink.connector.iggy;

import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.api.connector.source.Source;
import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.util.Preconditions;

import java.io.Serializable;
import java.time.Duration;

/**
 * Flink Source for Apache Iggy.
 *
 * <p>Consumes messages from all partitions of an Iggy topic with round-robin
 * assignment to readers and periodic partition discovery.
 *
 * <pre>{@code
 * IggySource<byte[]> source = IggySource.<byte[]>builder()
 *     .setStream("crypto")
 *     .setTopic("prices")
 *     .setDeserializer(payload -> payload)
 *     .build();
 * }</pre>
 *
 * @param <T> the output record type
 */
public class IggySource<T> implements Source<T, IggySplit, IggyEnumeratorState>, Serializable {

    private static final long serialVersionUID = 1L;

    private final IggyConnectionConfig connectionConfig;
    private final String streamId;
    private final String topicId;
    private final Duration discoveryInterval;
    private final Duration pollBackoff;
    private final int batchSize;
    private final long consumerId;
    private final IggyDeserializationSchema<T> deserializer;

    private IggySource(Builder<T> b) {
        this.connectionConfig = new IggyConnectionConfig(
                b.host, b.port, b.username, b.password, b.tls, b.tlsCertificatePath);
        this.streamId = Preconditions.checkNotNull(b.streamId, "Stream must be set");
        this.topicId = Preconditions.checkNotNull(b.topicId, "Topic must be set");
        this.discoveryInterval = Preconditions.checkNotNull(b.discoveryInterval);
        this.pollBackoff = Preconditions.checkNotNull(b.pollBackoff);
        this.batchSize = b.batchSize;
        this.consumerId = b.consumerId;
        this.deserializer = Preconditions.checkNotNull(b.deserializer, "Deserializer must be set");
    }

    public static <T> Builder<T> builder() {
        return new Builder<>();
    }

    /** Convenience factory for raw byte output with default connection settings. */
    public static IggySource<byte[]> forBytes(String streamId, String topicId) {
        return IggySource.<byte[]>builder()
                .setStream(streamId)
                .setTopic(topicId)
                .setDeserializer(payload -> payload)
                .build();
    }

    @Override
    public Boundedness getBoundedness() {
        return Boundedness.CONTINUOUS_UNBOUNDED;
    }

    @Override
    public SourceReader<T, IggySplit> createReader(SourceReaderContext readerContext) {
        return new IggySourceReader<>(
                readerContext, connectionConfig, pollBackoff.toMillis(),
                batchSize, consumerId, deserializer);
    }

    @Override
    public SplitEnumerator<IggySplit, IggyEnumeratorState> createEnumerator(
            SplitEnumeratorContext<IggySplit> enumContext) {
        return new IggySplitEnumerator(
                enumContext, connectionConfig, streamId, topicId,
                discoveryInterval.toMillis());
    }

    @Override
    public SplitEnumerator<IggySplit, IggyEnumeratorState> restoreEnumerator(
            SplitEnumeratorContext<IggySplit> enumContext,
            IggyEnumeratorState checkpoint) {
        return new IggySplitEnumerator(
                enumContext, connectionConfig, streamId, topicId,
                discoveryInterval.toMillis(), checkpoint);
    }

    @Override
    public SimpleVersionedSerializer<IggySplit> getSplitSerializer() {
        return new IggySplitSerializer();
    }

    @Override
    public SimpleVersionedSerializer<IggyEnumeratorState> getEnumeratorCheckpointSerializer() {
        return new IggyEnumeratorStateSerializer();
    }

    /** Fluent builder for {@link IggySource}. */
    public static class Builder<T> {
        private String host = "localhost";
        private int port = 8090;
        private String username = "iggy";
        private String password = "iggy";
        private String streamId;
        private String topicId;
        private boolean tls;
        private String tlsCertificatePath;
        private Duration discoveryInterval = Duration.ofMinutes(1);
        private Duration pollBackoff = Duration.ofMillis(100);
        private int batchSize = 100;
        private long consumerId = 1L;
        private IggyDeserializationSchema<T> deserializer;

        public Builder<T> setHost(String host) { this.host = host; return this; }
        public Builder<T> setPort(int port) {
            Preconditions.checkArgument(port > 0 && port <= 65535, "Invalid port: %s", port);
            this.port = port; return this;
        }
        public Builder<T> setCredentials(String username, String password) {
            this.username = username; this.password = password; return this;
        }
        public Builder<T> setStream(String streamId) { this.streamId = streamId; return this; }
        public Builder<T> setTopic(String topicId) { this.topicId = topicId; return this; }
        public Builder<T> enableTls() { this.tls = true; return this; }
        public Builder<T> setTlsCertificate(String path) { this.tlsCertificatePath = path; return this; }
        public Builder<T> setDiscoveryInterval(Duration interval) { this.discoveryInterval = interval; return this; }
        public Builder<T> setPollBackoff(Duration backoff) { this.pollBackoff = backoff; return this; }
        public Builder<T> setBatchSize(int batchSize) {
            Preconditions.checkArgument(batchSize > 0, "Batch size must be positive");
            this.batchSize = batchSize; return this;
        }
        public Builder<T> setConsumerId(long consumerId) { this.consumerId = consumerId; return this; }
        public Builder<T> setDeserializer(IggyDeserializationSchema<T> d) { this.deserializer = d; return this; }

        public IggySource<T> build() {
            return new IggySource<>(this);
        }
    }
}
