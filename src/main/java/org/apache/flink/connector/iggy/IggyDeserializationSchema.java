package org.apache.flink.connector.iggy;

import org.apache.flink.metrics.MetricGroup;

import java.io.Serializable;

/**
 * Converts raw Iggy message bytes into output records.
 *
 * @param <T> the output record type
 */
@FunctionalInterface
public interface IggyDeserializationSchema<T> extends Serializable {

    /**
     * Called once when the reader starts. Override to initialize stateful
     * deserializers (e.g., Flink's JSON format).
     */
    default void open(MetricGroup metricGroup) throws Exception {}

    T deserialize(byte[] payload) throws Exception;
}
