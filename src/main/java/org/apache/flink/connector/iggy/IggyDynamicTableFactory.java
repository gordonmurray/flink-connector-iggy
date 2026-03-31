package org.apache.flink.connector.iggy;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.table.connector.format.DecodingFormat;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.factories.DeserializationFormatFactory;
import org.apache.flink.table.factories.DynamicTableSourceFactory;
import org.apache.flink.table.factories.FactoryUtil;

import java.time.Duration;
import java.util.HashSet;
import java.util.Set;

/**
 * Factory for creating {@link IggyDynamicTableSource} from Flink SQL DDL.
 *
 * <p>Usage:
 * <pre>{@code
 * CREATE TABLE iggy_prices (
 *   pair STRING,
 *   price STRING,
 *   volume_24h STRING,
 *   `time` STRING
 * ) WITH (
 *   'connector' = 'iggy',
 *   'stream'    = 'crypto',
 *   'topic'     = 'prices',
 *   'format'    = 'json'
 * );
 * }</pre>
 *
 * <p>Registered via SPI in {@code META-INF/services/org.apache.flink.table.factories.Factory}.
 */
public class IggyDynamicTableFactory implements DynamicTableSourceFactory {

    public static final String IDENTIFIER = "iggy";

    public static final ConfigOption<String> HOST = ConfigOptions
            .key("host").stringType().defaultValue("localhost");
    public static final ConfigOption<Integer> PORT = ConfigOptions
            .key("port").intType().defaultValue(8090);
    public static final ConfigOption<String> USERNAME = ConfigOptions
            .key("username").stringType().defaultValue("iggy");
    public static final ConfigOption<String> PASSWORD = ConfigOptions
            .key("password").stringType().defaultValue("iggy");
    public static final ConfigOption<String> STREAM = ConfigOptions
            .key("stream").stringType().noDefaultValue();
    public static final ConfigOption<String> TOPIC = ConfigOptions
            .key("topic").stringType().noDefaultValue();
    public static final ConfigOption<Boolean> TLS = ConfigOptions
            .key("tls").booleanType().defaultValue(false);
    public static final ConfigOption<String> TLS_CERTIFICATE = ConfigOptions
            .key("tls.certificate").stringType().noDefaultValue();
    public static final ConfigOption<Long> POLL_TIMEOUT = ConfigOptions
            .key("poll.timeout").longType().defaultValue(5000L)
            .withDescription("Timeout in milliseconds for each poll call to Iggy. "
                    + "Prevents blocking on empty partitions.");
    public static final ConfigOption<String> STARTING_OFFSET = ConfigOptions
            .key("starting-offset").stringType().defaultValue("earliest")
            .withDescription("Starting offset policy for new splits: "
                    + "'earliest', 'latest', or 'specific-offset:<long>'.");

    @Override
    public String factoryIdentifier() {
        return IDENTIFIER;
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        Set<ConfigOption<?>> options = new HashSet<>();
        options.add(STREAM);
        options.add(TOPIC);
        return options;
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        Set<ConfigOption<?>> options = new HashSet<>();
        options.add(HOST);
        options.add(PORT);
        options.add(USERNAME);
        options.add(PASSWORD);
        options.add(TLS);
        options.add(TLS_CERTIFICATE);
        options.add(POLL_TIMEOUT);
        options.add(STARTING_OFFSET);
        options.add(FactoryUtil.FORMAT);
        return options;
    }

    @Override
    public DynamicTableSource createDynamicTableSource(Context context) {
        FactoryUtil.TableFactoryHelper helper = FactoryUtil.createTableFactoryHelper(this, context);

        DecodingFormat<DeserializationSchema<RowData>> decodingFormat =
                helper.discoverDecodingFormat(DeserializationFormatFactory.class,
                        FactoryUtil.FORMAT);

        helper.validate();

        String host = helper.getOptions().get(HOST);
        int port = helper.getOptions().get(PORT);
        String username = helper.getOptions().get(USERNAME);
        String password = helper.getOptions().get(PASSWORD);
        String stream = helper.getOptions().get(STREAM);
        String topic = helper.getOptions().get(TOPIC);
        boolean tlsEnabled = helper.getOptions().get(TLS);
        String tlsCert = helper.getOptions().get(TLS_CERTIFICATE);
        long pollTimeoutMs = helper.getOptions().get(POLL_TIMEOUT);
        IggyOffsetSpec offsetSpec = parseStartingOffset(helper.getOptions().get(STARTING_OFFSET));

        return new IggyDynamicTableSource(
                host, port, username, password, stream, topic,
                tlsEnabled, tlsCert,
                Duration.ofMillis(pollTimeoutMs),
                offsetSpec,
                decodingFormat,
                context.getCatalogTable().getResolvedSchema().toPhysicalRowDataType());
    }

    static IggyOffsetSpec parseStartingOffset(String value) {
        if (value == null || value.isBlank()) {
            return IggyOffsetSpec.earliest();
        }
        String trimmed = value.trim().toLowerCase();
        if ("earliest".equals(trimmed)) {
            return IggyOffsetSpec.earliest();
        }
        if ("latest".equals(trimmed)) {
            return IggyOffsetSpec.latest();
        }
        if (trimmed.startsWith("specific-offset:")) {
            String offsetStr = trimmed.substring("specific-offset:".length()).trim();
            try {
                return IggyOffsetSpec.specificOffset(Long.parseLong(offsetStr));
            } catch (NumberFormatException e) {
                throw new IllegalArgumentException(
                        "Invalid specific-offset value: '" + offsetStr + "'. Expected a long.", e);
            }
        }
        throw new IllegalArgumentException(
                "Unknown starting-offset value: '" + value
                        + "'. Expected 'earliest', 'latest', or 'specific-offset:<long>'.");
    }
}
