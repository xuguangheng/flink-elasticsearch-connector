//
// Source code recreated from a .class file by IntelliJ IDEA
// (powered by Fernflower decompiler)
//

package org.apache.flink.streaming.connectors.kafka.table;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.streaming.connectors.kafka.config.StartupMode;
import org.apache.flink.streaming.connectors.kafka.partitioner.FlinkKafkaPartitioner;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.api.constraints.UniqueConstraint;
import org.apache.flink.table.catalog.CatalogTable;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.format.DecodingFormat;
import org.apache.flink.table.connector.format.EncodingFormat;
import org.apache.flink.table.connector.format.Format;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.factories.DeserializationFormatFactory;
import org.apache.flink.table.factories.DynamicTableSinkFactory;
import org.apache.flink.table.factories.DynamicTableSourceFactory;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.factories.FactoryUtil.TableFactoryHelper;
import org.apache.flink.table.factories.SerializationFormatFactory;
import org.apache.flink.table.types.DataType;
import org.apache.flink.types.RowKind;

import java.time.Duration;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Properties;
import java.util.Set;

import static org.apache.flink.streaming.connectors.kafka.table.TriggerKafkaContants.TRIGGER_KEY_FORMAT;
import static org.apache.flink.streaming.connectors.kafka.table.TriggerKafkaContants.TRIGGER_PROPS_BOOTSTRAP_SERVERS;
import static org.apache.flink.streaming.connectors.kafka.table.TriggerKafkaContants.TRIGGER_TOPIC;
import static org.apache.flink.streaming.connectors.kafka.table.TriggerKafkaContants.TRIGGER_VALUE_FORMAT;

public class UpsertKafkaDynamicTableFactoryTest implements DynamicTableSourceFactory, DynamicTableSinkFactory {
    public static final String IDENTIFIER = "upsert-kafka-test";

    public UpsertKafkaDynamicTableFactoryTest() {
    }

    public String factoryIdentifier() {
        return IDENTIFIER;
    }


    public Set<ConfigOption<?>> requiredOptions() {
        Set<ConfigOption<?>> options = new HashSet();
        options.add(KafkaOptions.PROPS_BOOTSTRAP_SERVERS);
        options.add(KafkaOptions.TOPIC);
        options.add(KafkaOptions.KEY_FORMAT);
        options.add(KafkaOptions.VALUE_FORMAT);
        return options;
    }

    public Set<ConfigOption<?>> optionalOptions() {
        Set<ConfigOption<?>> options = new HashSet();
        options.add(KafkaOptions.KEY_FIELDS_PREFIX);
        options.add(KafkaOptions.VALUE_FIELDS_INCLUDE);
        options.add(FactoryUtil.SINK_PARALLELISM);
        options.add(KafkaOptions.SINK_BUFFER_FLUSH_INTERVAL);
        options.add(KafkaOptions.SINK_BUFFER_FLUSH_MAX_ROWS);
        options.add(TRIGGER_TOPIC);
        options.add(TRIGGER_KEY_FORMAT);
        options.add(TRIGGER_VALUE_FORMAT);
        options.add(TRIGGER_PROPS_BOOTSTRAP_SERVERS);
        return options;
    }

    public DynamicTableSource createDynamicTableSource(Context context) {
        TableFactoryHelper helper = FactoryUtil.createTableFactoryHelper(this, context);
        ReadableConfig tableOptions = helper.getOptions();
        DecodingFormat<DeserializationSchema<RowData>> keyDecodingFormat = helper.discoverDecodingFormat(DeserializationFormatFactory.class, KafkaOptions.KEY_FORMAT);
        DecodingFormat<DeserializationSchema<RowData>> valueDecodingFormat = helper.discoverDecodingFormat(DeserializationFormatFactory.class, KafkaOptions.VALUE_FORMAT);
        helper.validateExcept(new String[]{"properties."});
        TableSchema schema = context.getCatalogTable().getSchema();
        validateSource(tableOptions, keyDecodingFormat, valueDecodingFormat, schema);
        Tuple2<int[], int[]> keyValueProjections = this.createKeyValueProjections(context.getCatalogTable());
        String keyPrefix = (String)tableOptions.getOptional(KafkaOptions.KEY_FIELDS_PREFIX).orElse(null);
        Properties properties = KafkaOptions.getKafkaProperties(context.getCatalogTable().getOptions());
        StartupMode earliest = StartupMode.EARLIEST;
        return new KafkaDynamicSource(schema.toPhysicalRowDataType(), keyDecodingFormat, new UpsertKafkaDynamicTableFactoryTest.DecodingFormatWrapper(valueDecodingFormat), (int[])keyValueProjections.f0, (int[])keyValueProjections.f1, keyPrefix, KafkaOptions.getSourceTopics(tableOptions), KafkaOptions.getSourceTopicPattern(tableOptions), properties, earliest, Collections.emptyMap(), 0L, true);
    }

    public DynamicTableSink createDynamicTableSink(Context context) {
        TableFactoryHelper helper = FactoryUtil.createTableFactoryHelper(this, KafkaOptions.autoCompleteSchemaRegistrySubject(context));
        ReadableConfig tableOptions = helper.getOptions();

        DecodingFormat<DeserializationSchema<RowData>> keyDecodingFormat = helper.discoverDecodingFormat(DeserializationFormatFactory.class, KafkaOptions.KEY_FORMAT);
        DecodingFormat<DeserializationSchema<RowData>> valueDecodingFormat = helper.discoverDecodingFormat(DeserializationFormatFactory.class, KafkaOptions.VALUE_FORMAT);
        helper.validateExcept(new String[]{"properties."});
        TableSchema schema = context.getCatalogTable().getSchema();
        validateSource(tableOptions, keyDecodingFormat, valueDecodingFormat, schema);
        Tuple2<int[], int[]> keyValueProjections = this.createKeyValueProjections(context.getCatalogTable());
        String keyPrefix = (String)tableOptions.getOptional(KafkaOptions.KEY_FIELDS_PREFIX).orElse(null);
        Properties properties = KafkaOptions.getKafkaProperties(context.getCatalogTable().getOptions());
        StartupMode earliest = StartupMode.EARLIEST;
        KafkaDynamicSource triggerSource = new KafkaDynamicSource(schema.toPhysicalRowDataType(), keyDecodingFormat, new UpsertKafkaDynamicTableFactoryTest.DecodingFormatWrapper(valueDecodingFormat), (int[])keyValueProjections.f0, (int[])keyValueProjections.f1, keyPrefix, (List)tableOptions.get(TRIGGER_TOPIC), KafkaOptions.getSourceTopicPattern(tableOptions), properties, earliest, Collections.emptyMap(), 0L, true);

        EncodingFormat<SerializationSchema<RowData>> keyEncodingFormat = helper.discoverEncodingFormat(SerializationFormatFactory.class, KafkaOptions.KEY_FORMAT);
        EncodingFormat<SerializationSchema<RowData>> valueEncodingFormat = helper.discoverEncodingFormat(SerializationFormatFactory.class, KafkaOptions.VALUE_FORMAT);
        validateSink(tableOptions, keyEncodingFormat, valueEncodingFormat, schema);
        Integer parallelism = (Integer)tableOptions.get(FactoryUtil.SINK_PARALLELISM);
        int batchSize = (Integer)tableOptions.get(KafkaOptions.SINK_BUFFER_FLUSH_MAX_ROWS);
        Duration batchInterval = (Duration)tableOptions.get(KafkaOptions.SINK_BUFFER_FLUSH_INTERVAL);
        SinkBufferFlushMode flushMode = new SinkBufferFlushMode(batchSize, batchInterval.toMillis());
        return new KafkaDynamicSinkTest(schema.toPhysicalRowDataType(), schema.toPhysicalRowDataType(), keyEncodingFormat, new UpsertKafkaDynamicTableFactoryTest.EncodingFormatWrapper(valueEncodingFormat), (int[])keyValueProjections.f0, (int[])keyValueProjections.f1, keyPrefix, (String)((List)tableOptions.get(KafkaOptions.TOPIC)).get(0), properties, (FlinkKafkaPartitioner)null, KafkaSinkSemantic.AT_LEAST_ONCE, true, flushMode, parallelism, triggerSource);
    }

    private Tuple2<int[], int[]> createKeyValueProjections(CatalogTable catalogTable) {
        TableSchema schema = catalogTable.getSchema();
        List<String> keyFields = ((UniqueConstraint)schema.getPrimaryKey().get()).getColumns();
        DataType physicalDataType = schema.toPhysicalRowDataType();
        Configuration tableOptions = Configuration.fromMap(catalogTable.getOptions());
        tableOptions.set(KafkaOptions.KEY_FIELDS, keyFields);
        int[] keyProjection = KafkaOptions.createKeyFormatProjection(tableOptions, physicalDataType);
        int[] valueProjection = KafkaOptions.createValueFormatProjection(tableOptions, physicalDataType);
        return Tuple2.of(keyProjection, valueProjection);
    }

    private static void validateSource(ReadableConfig tableOptions, Format keyFormat, Format valueFormat, TableSchema schema) {
        validateTopic(tableOptions);
        validateFormat(keyFormat, valueFormat, tableOptions);
        validatePKConstraints(schema);
    }

    private static void validateSink(ReadableConfig tableOptions, Format keyFormat, Format valueFormat, TableSchema schema) {
        validateTopic(tableOptions);
        validateFormat(keyFormat, valueFormat, tableOptions);
        validatePKConstraints(schema);
        validateSinkBufferFlush(tableOptions);
    }

    private static void validateTopic(ReadableConfig tableOptions) {
        List<String> topic = (List)tableOptions.get(KafkaOptions.TOPIC);
        if (topic.size() > 1) {
            throw new ValidationException("The 'upsert-kafka' connector doesn't support topic list now. Please use single topic as the value of the parameter 'topic'.");
        }
    }

    private static void validateFormat(Format keyFormat, Format valueFormat, ReadableConfig tableOptions) {
        String identifier;
        if (!keyFormat.getChangelogMode().containsOnly(RowKind.INSERT)) {
            identifier = (String)tableOptions.get(KafkaOptions.KEY_FORMAT);
            throw new ValidationException(String.format("'upsert-kafka' connector doesn't support '%s' as key format, because '%s' is not in insert-only mode.", identifier, identifier));
        } else if (!valueFormat.getChangelogMode().containsOnly(RowKind.INSERT)) {
            identifier = (String)tableOptions.get(KafkaOptions.VALUE_FORMAT);
            throw new ValidationException(String.format("'upsert-kafka' connector doesn't support '%s' as value format, because '%s' is not in insert-only mode.", identifier, identifier));
        }
    }

    private static void validatePKConstraints(TableSchema schema) {
        if (!schema.getPrimaryKey().isPresent()) {
            throw new ValidationException("'upsert-kafka' tables require to define a PRIMARY KEY constraint. The PRIMARY KEY specifies which columns should be read from or write to the Kafka message key. The PRIMARY KEY also defines records in the 'upsert-kafka' table should update or delete on which keys.");
        }
    }

    private static void validateSinkBufferFlush(ReadableConfig tableOptions) {
        int flushMaxRows = (Integer)tableOptions.get(KafkaOptions.SINK_BUFFER_FLUSH_MAX_ROWS);
        long flushIntervalMs = ((Duration)tableOptions.get(KafkaOptions.SINK_BUFFER_FLUSH_INTERVAL)).toMillis();
        if (flushMaxRows <= 0 || flushIntervalMs <= 0L) {
            if (flushMaxRows > 0 || flushIntervalMs > 0L) {
                throw new ValidationException(String.format("'%s' and '%s' must be set to be greater than zero together to enable sink buffer flushing.", KafkaOptions.SINK_BUFFER_FLUSH_MAX_ROWS.key(), KafkaOptions.SINK_BUFFER_FLUSH_INTERVAL.key()));
            }
        }
    }

    protected static class EncodingFormatWrapper implements EncodingFormat<SerializationSchema<RowData>> {
        private final EncodingFormat<SerializationSchema<RowData>> innerEncodingFormat;
        public static final ChangelogMode SINK_CHANGELOG_MODE;

        public EncodingFormatWrapper(EncodingFormat<SerializationSchema<RowData>> innerEncodingFormat) {
            this.innerEncodingFormat = innerEncodingFormat;
        }

        public SerializationSchema<RowData> createRuntimeEncoder(org.apache.flink.table.connector.sink.DynamicTableSink.Context context, DataType consumedDataType) {
            return (SerializationSchema)this.innerEncodingFormat.createRuntimeEncoder(context, consumedDataType);
        }

        public ChangelogMode getChangelogMode() {
            return SINK_CHANGELOG_MODE;
        }

        public boolean equals(Object obj) {
            if (obj == this) {
                return true;
            } else if (obj != null && this.getClass() == obj.getClass()) {
                UpsertKafkaDynamicTableFactoryTest.EncodingFormatWrapper that = (UpsertKafkaDynamicTableFactoryTest.EncodingFormatWrapper)obj;
                return Objects.equals(this.innerEncodingFormat, that.innerEncodingFormat);
            } else {
                return false;
            }
        }

        public int hashCode() {
            return Objects.hash(new Object[]{this.innerEncodingFormat});
        }

        static {
            SINK_CHANGELOG_MODE = ChangelogMode.newBuilder().addContainedKind(RowKind.INSERT).addContainedKind(RowKind.UPDATE_AFTER).addContainedKind(RowKind.DELETE).build();
        }
    }

    protected static class DecodingFormatWrapper implements DecodingFormat<DeserializationSchema<RowData>> {
        private final DecodingFormat<DeserializationSchema<RowData>> innerDecodingFormat;
        private static final ChangelogMode SOURCE_CHANGELOG_MODE;

        public DecodingFormatWrapper(DecodingFormat<DeserializationSchema<RowData>> innerDecodingFormat) {
            this.innerDecodingFormat = innerDecodingFormat;
        }

        public DeserializationSchema<RowData> createRuntimeDecoder(org.apache.flink.table.connector.source.DynamicTableSource.Context context, DataType producedDataType) {
            return (DeserializationSchema)this.innerDecodingFormat.createRuntimeDecoder(context, producedDataType);
        }

        public ChangelogMode getChangelogMode() {
            return SOURCE_CHANGELOG_MODE;
        }

        public boolean equals(Object obj) {
            if (obj == this) {
                return true;
            } else if (obj != null && this.getClass() == obj.getClass()) {
                UpsertKafkaDynamicTableFactoryTest.DecodingFormatWrapper that = (UpsertKafkaDynamicTableFactoryTest.DecodingFormatWrapper)obj;
                return Objects.equals(this.innerDecodingFormat, that.innerDecodingFormat);
            } else {
                return false;
            }
        }

        public int hashCode() {
            return Objects.hash(new Object[]{this.innerDecodingFormat});
        }

        static {
            SOURCE_CHANGELOG_MODE = ChangelogMode.newBuilder().addContainedKind(RowKind.UPDATE_AFTER).addContainedKind(RowKind.DELETE).build();
        }
    }
    
}
