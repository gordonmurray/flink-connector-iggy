package org.apache.flink.connector.iggy;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.format.DecodingFormat;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.ScanTableSource;
import org.apache.flink.table.connector.source.SourceProvider;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.types.RowKind;

/**
 * Scan table source for Apache Iggy.
 */
public class IggyDynamicTableSource implements ScanTableSource {

    private final String host;
    private final int port;
    private final String username;
    private final String password;
    private final String stream;
    private final String topic;
    private final boolean tls;
    private final String tlsCertificatePath;
    private final DecodingFormat<DeserializationSchema<RowData>> decodingFormat;
    private final DataType physicalDataType;

    public IggyDynamicTableSource(
            String host, int port,
            String username, String password,
            String stream, String topic,
            boolean tls, String tlsCertificatePath,
            DecodingFormat<DeserializationSchema<RowData>> decodingFormat,
            DataType physicalDataType) {
        this.host = host;
        this.port = port;
        this.username = username;
        this.password = password;
        this.stream = stream;
        this.topic = topic;
        this.tls = tls;
        this.tlsCertificatePath = tlsCertificatePath;
        this.decodingFormat = decodingFormat;
        this.physicalDataType = physicalDataType;
    }

    @Override
    public ChangelogMode getChangelogMode() {
        return ChangelogMode.newBuilder()
                .addContainedKind(RowKind.INSERT)
                .build();
    }

    @Override
    public ScanRuntimeProvider getScanRuntimeProvider(ScanContext context) {
        DeserializationSchema<RowData> flinkSchema =
                decodingFormat.createRuntimeDecoder(context, physicalDataType);

        var iggySchema = new FlinkSchemaAdapter(flinkSchema);

        IggySource.Builder<RowData> builder = IggySource.<RowData>builder()
                .setHost(host)
                .setPort(port)
                .setCredentials(username, password)
                .setStream(stream)
                .setTopic(topic)
                .setDeserializer(iggySchema);

        if (tls) {
            builder.enableTls();
            if (tlsCertificatePath != null) {
                builder.setTlsCertificate(tlsCertificatePath);
            }
        }

        return SourceProvider.of(builder.build());
    }

    @Override
    public DynamicTableSource copy() {
        return new IggyDynamicTableSource(
                host, port, username, password, stream, topic,
                tls, tlsCertificatePath, decodingFormat, physicalDataType);
    }

    @Override
    public String asSummaryString() {
        return "Iggy[" + stream + "/" + topic + "]";
    }

    /**
     * Static adapter so that serialization of the IggySource does not
     * transitively capture the non-serializable IggyDynamicTableSource.
     */
    private static class FlinkSchemaAdapter implements IggyDeserializationSchema<RowData> {
        private static final long serialVersionUID = 1L;
        private final DeserializationSchema<RowData> delegate;

        FlinkSchemaAdapter(DeserializationSchema<RowData> delegate) {
            this.delegate = delegate;
        }

        @Override
        public void open(org.apache.flink.metrics.MetricGroup metricGroup) throws Exception {
            delegate.open(new DeserializationSchema.InitializationContext() {
                @Override
                public org.apache.flink.metrics.MetricGroup getMetricGroup() {
                    return metricGroup;
                }

                @Override
                public org.apache.flink.util.UserCodeClassLoader getUserCodeClassLoader() {
                    var cl = Thread.currentThread().getContextClassLoader();
                    return new org.apache.flink.util.UserCodeClassLoader() {
                        @Override public ClassLoader asClassLoader() { return cl; }
                        @Override public void registerReleaseHookIfAbsent(String n, Runnable r) {}
                    };
                }
            });
        }

        @Override
        public RowData deserialize(byte[] payload) throws Exception {
            return delegate.deserialize(payload);
        }
    }
}
