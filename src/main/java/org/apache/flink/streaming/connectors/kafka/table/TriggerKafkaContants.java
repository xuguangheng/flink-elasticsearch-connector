package org.apache.flink.streaming.connectors.kafka.table;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;

import java.util.List;

import static org.apache.flink.table.factories.FactoryUtil.FORMAT_SUFFIX;

public class TriggerKafkaContants {
    public static final String TRIGGER_PREFIX = "trigger.";

    public static final ConfigOption<List<String>> TRIGGER_TOPIC =
            ConfigOptions.key(TRIGGER_PREFIX + "topic")
                    .stringType()
                    .asList()
                    .noDefaultValue()
                    .withDescription(
                            "Trigger topic names from which the table is read.");

    public static final ConfigOption<String> TRIGGER_PROPS_BOOTSTRAP_SERVERS =
            ConfigOptions.key(TRIGGER_PREFIX + "properties.bootstrap.servers")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Kafka server connection string for trigger topic");

    public static final ConfigOption<String> TRIGGER_KEY_FORMAT =
            ConfigOptions.key(TRIGGER_PREFIX + "key" + FORMAT_SUFFIX)
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "Defines the format identifier for encoding key data. "
                                    + "The identifier is used to discover a suitable format factory.");

    public static final ConfigOption<String> TRIGGER_VALUE_FORMAT =
            ConfigOptions.key(TRIGGER_PREFIX + "value" + FORMAT_SUFFIX)
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "Defines the format identifier for encoding value data. "
                                    + "The identifier is used to discover a suitable format factory.");
}
