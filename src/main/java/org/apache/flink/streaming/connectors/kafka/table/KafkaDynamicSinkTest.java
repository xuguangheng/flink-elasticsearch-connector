//
// Source code recreated from a .class file by IntelliJ IDEA
// (powered by Fernflower decompiler)
//

package org.apache.flink.streaming.connectors.kafka.table;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer.Semantic;
import org.apache.flink.streaming.connectors.kafka.partitioner.FlinkKafkaPartitioner;
import org.apache.flink.streaming.connectors.kafka.table.DynamicKafkaSerializationSchema.MetadataConverter;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.format.EncodingFormat;
import org.apache.flink.table.connector.sink.DataStreamSinkProvider;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.sink.SinkFunctionProvider;
import org.apache.flink.table.connector.sink.abilities.SupportsWritingMetadata;
import org.apache.flink.table.connector.source.SourceFunctionProvider;
import org.apache.flink.table.data.ArrayData;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.MapData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.RowData.FieldGetter;
import org.apache.flink.table.runtime.connector.source.ScanRuntimeProviderContext;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.utils.DataTypeUtils;
import org.apache.flink.types.RowKind;
import org.apache.flink.util.Collector;
import org.apache.flink.util.Preconditions;
import org.apache.kafka.common.header.Header;

import javax.annotation.Nullable;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@Internal
public class KafkaDynamicSinkTest implements DynamicTableSink, SupportsWritingMetadata {
    protected List<String> metadataKeys;
    protected DataType consumedDataType;
    protected final DataType physicalDataType;
    @Nullable
    protected final EncodingFormat<SerializationSchema<RowData>> keyEncodingFormat;
    protected final EncodingFormat<SerializationSchema<RowData>> valueEncodingFormat;
    protected final int[] keyProjection;
    protected final int[] valueProjection;
    @Nullable
    protected final String keyPrefix;
    protected final String topic;
    protected final Properties properties;
    @Nullable
    protected final FlinkKafkaPartitioner<RowData> partitioner;
    protected final KafkaSinkSemantic semantic;
    protected final boolean upsertMode;
    protected final SinkBufferFlushMode flushMode;
    @Nullable
    protected final Integer parallelism;
    @Nullable
    protected final KafkaDynamicSource triggerSource;

    public KafkaDynamicSinkTest(DataType consumedDataType, DataType physicalDataType, @Nullable EncodingFormat<SerializationSchema<RowData>> keyEncodingFormat, EncodingFormat<SerializationSchema<RowData>> valueEncodingFormat, int[] keyProjection, int[] valueProjection, @Nullable String keyPrefix, String topic, Properties properties, @Nullable FlinkKafkaPartitioner<RowData> partitioner, KafkaSinkSemantic semantic, boolean upsertMode, SinkBufferFlushMode flushMode, @Nullable Integer parallelism, KafkaDynamicSource triggerSource) {
        this.consumedDataType = (DataType)Preconditions.checkNotNull(consumedDataType, "Consumed data type must not be null.");
        this.physicalDataType = (DataType)Preconditions.checkNotNull(physicalDataType, "Physical data type must not be null.");
        this.keyEncodingFormat = keyEncodingFormat;
        this.valueEncodingFormat = (EncodingFormat)Preconditions.checkNotNull(valueEncodingFormat, "Value encoding format must not be null.");
        this.keyProjection = (int[])Preconditions.checkNotNull(keyProjection, "Key projection must not be null.");
        this.valueProjection = (int[])Preconditions.checkNotNull(valueProjection, "Value projection must not be null.");
        this.keyPrefix = keyPrefix;
        this.metadataKeys = Collections.emptyList();
        this.topic = (String)Preconditions.checkNotNull(topic, "Topic must not be null.");
        this.properties = (Properties)Preconditions.checkNotNull(properties, "Properties must not be null.");
        this.partitioner = partitioner;
        this.semantic = (KafkaSinkSemantic)Preconditions.checkNotNull(semantic, "Semantic must not be null.");
        this.upsertMode = upsertMode;
        this.flushMode = (SinkBufferFlushMode)Preconditions.checkNotNull(flushMode);
        if (flushMode.isEnabled() && !upsertMode) {
            throw new IllegalArgumentException("Sink buffer flush is only supported in upsert-kafka.");
        } else {
            this.parallelism = parallelism;
        }
        this.triggerSource = triggerSource;
    }

    public ChangelogMode getChangelogMode(ChangelogMode requestedMode) {
        return this.valueEncodingFormat.getChangelogMode();
    }

    public SinkRuntimeProvider getSinkRuntimeProvider(Context context) {
        SerializationSchema<RowData> keySerialization = this.createSerialization(context, this.keyEncodingFormat, this.keyProjection, this.keyPrefix);
        SerializationSchema<RowData> valueSerialization = this.createSerialization(context, this.valueEncodingFormat, this.valueProjection, (String)null);
        FlinkKafkaProducer<RowData> kafkaProducer = this.createKafkaProducer(keySerialization, valueSerialization);
        final SinkFunctionProvider originalSinkProvider;
        if (this.flushMode.isEnabled() && this.upsertMode) {
            BufferedUpsertSinkFunction buffedSinkFunction = new BufferedUpsertSinkFunction(kafkaProducer, this.physicalDataType, this.keyProjection, context.createTypeInformation(this.consumedDataType), this.flushMode);
            originalSinkProvider = SinkFunctionProvider.of(buffedSinkFunction, this.parallelism);
        } else {
            originalSinkProvider = SinkFunctionProvider.of(kafkaProducer, this.parallelism);
        }
        final FieldGetter[] keyFields = getKeyFieldGetter();
        return new DataStreamSinkProvider() {
            @Override
            public DataStreamSink<RowData> consumeDataStream(DataStream<RowData> dataStream) {
                SourceFunctionProvider sourceFunctionProvider = (SourceFunctionProvider)triggerSource.getScanRuntimeProvider(ScanRuntimeProviderContext.INSTANCE);
                DataStream<RowData> trigger = dataStream.getExecutionEnvironment()
                        .addSource(sourceFunctionProvider.createSourceFunction());

                DataStream<RowDataWithKey> triggerKeyedStream = trigger.map(rowData -> {
                    RowData keyRow = DynamicKafkaSerializationSchema.createProjectedRow(rowData, RowKind.INSERT, keyFields);
                    String key = Arrays.stream(keyFields)
                            .map(a -> a.getFieldOrNull(keyRow).toString())
                            .collect(Collectors.joining());
                    return new RowDataWithKey(key, rowData);
                }).keyBy(RowDataWithKey::getKey);


                DataStream<RowDataWithKey> keyedStream = dataStream.map(rowData -> {
                    RowData keyRow = DynamicKafkaSerializationSchema.createProjectedRow(rowData, RowKind.INSERT, keyFields);
                    String key = Arrays.stream(keyFields)
                            .map(a -> a.getFieldOrNull(keyRow).toString())
                            .collect(Collectors.joining());
                    return new RowDataWithKey(key, rowData);
                }).keyBy(RowDataWithKey::getKey);

                return keyedStream
                        .connect(triggerKeyedStream)
                        .process(new TriggerSinkFunction())
                        .addSink(originalSinkProvider.createSinkFunction());
            }
        };
    }

    public Map<String, DataType> listWritableMetadata() {
        Map<String, DataType> metadataMap = new LinkedHashMap();
        Stream.of(KafkaDynamicSinkTest.WritableMetadata.values()).forEachOrdered((m) -> {
            DataType var10000 = (DataType)metadataMap.put(m.key, m.dataType);
        });
        return metadataMap;
    }

    public void applyWritableMetadata(List<String> metadataKeys, DataType consumedDataType) {
        this.metadataKeys = metadataKeys;
        this.consumedDataType = consumedDataType;
    }

    public DynamicTableSink copy() {
        KafkaDynamicSinkTest copy = new KafkaDynamicSinkTest(this.consumedDataType, this.physicalDataType, this.keyEncodingFormat, this.valueEncodingFormat, this.keyProjection, this.valueProjection, this.keyPrefix, this.topic, this.properties, this.partitioner, this.semantic, this.upsertMode, this.flushMode, this.parallelism, this.triggerSource);
        copy.metadataKeys = this.metadataKeys;
        return copy;
    }

    public String asSummaryString() {
        return "Kafka table sink";
    }

    public boolean equals(Object o) {
        if (this == o) {
            return true;
        } else if (o != null && this.getClass() == o.getClass()) {
            KafkaDynamicSinkTest that = (KafkaDynamicSinkTest)o;
            return Objects.equals(this.metadataKeys, that.metadataKeys) && Objects.equals(this.consumedDataType, that.consumedDataType) && Objects.equals(this.physicalDataType, that.physicalDataType) && Objects.equals(this.keyEncodingFormat, that.keyEncodingFormat) && Objects.equals(this.valueEncodingFormat, that.valueEncodingFormat) && Arrays.equals(this.keyProjection, that.keyProjection) && Arrays.equals(this.valueProjection, that.valueProjection) && Objects.equals(this.keyPrefix, that.keyPrefix) && Objects.equals(this.topic, that.topic) && Objects.equals(this.properties, that.properties) && Objects.equals(this.partitioner, that.partitioner) && Objects.equals(this.semantic, that.semantic) && Objects.equals(this.upsertMode, that.upsertMode) && Objects.equals(this.flushMode, that.flushMode) && Objects.equals(this.parallelism, that.parallelism);
        } else {
            return false;
        }
    }

    public int hashCode() {
        return Objects.hash(new Object[]{this.metadataKeys, this.consumedDataType, this.physicalDataType, this.keyEncodingFormat, this.valueEncodingFormat, this.keyProjection, this.valueProjection, this.keyPrefix, this.topic, this.properties, this.partitioner, this.semantic, this.upsertMode, this.flushMode, this.parallelism});
    }

    protected FlinkKafkaProducer<RowData> createKafkaProducer(SerializationSchema<RowData> keySerialization, SerializationSchema<RowData> valueSerialization) {
        List<LogicalType> physicalChildren = this.physicalDataType.getLogicalType().getChildren();
        FieldGetter[] keyFieldGetters = getKeyFieldGetter();
        FieldGetter[] valueFieldGetters = getValueFieldGetter();

        int[] metadataPositions = Stream.of(KafkaDynamicSinkTest.WritableMetadata.values()).mapToInt((m) -> {
            int pos = this.metadataKeys.indexOf(m.key);
            return pos < 0 ? -1 : physicalChildren.size() + pos;
        }).toArray();
        boolean hasMetadata = this.metadataKeys.size() > 0;
        DynamicKafkaSerializationSchema kafkaSerializer = new DynamicKafkaSerializationSchema(this.topic, this.partitioner, keySerialization, valueSerialization, keyFieldGetters, valueFieldGetters, hasMetadata, metadataPositions, this.upsertMode);
        return new FlinkKafkaProducer(this.topic, kafkaSerializer, this.properties, Semantic.valueOf(this.semantic.toString()), 5);
    }

    private FieldGetter[] getKeyFieldGetter() {
        List<LogicalType> physicalChildren = this.physicalDataType.getLogicalType().getChildren();
        return (FieldGetter[])Arrays.stream(this.keyProjection).mapToObj((targetField) -> {
            return RowData.createFieldGetter((LogicalType)physicalChildren.get(targetField), targetField);
        }).toArray((a) -> {
            return new FieldGetter[a];
        });
    }

    private FieldGetter[] getValueFieldGetter() {
        List<LogicalType> physicalChildren = this.physicalDataType.getLogicalType().getChildren();
        return (FieldGetter[])Arrays.stream(this.valueProjection).mapToObj((targetField) -> {
            return RowData.createFieldGetter((LogicalType)physicalChildren.get(targetField), targetField);
        }).toArray((a) -> {
            return new FieldGetter[a];
        });
    }


    @Nullable
    private SerializationSchema<RowData> createSerialization(Context context, @Nullable EncodingFormat<SerializationSchema<RowData>> format, int[] projection, @Nullable String prefix) {
        if (format == null) {
            return null;
        } else {
            DataType physicalFormatDataType = DataTypeUtils.projectRow(this.physicalDataType, projection);
            if (prefix != null) {
                physicalFormatDataType = DataTypeUtils.stripRowPrefix(physicalFormatDataType, prefix);
            }

            return (SerializationSchema)format.createRuntimeEncoder(context, physicalFormatDataType);
        }
    }

    private static class KafkaHeader implements Header {
        private final String key;
        private final byte[] value;

        KafkaHeader(String key, byte[] value) {
            this.key = key;
            this.value = value;
        }

        public String key() {
            return this.key;
        }

        public byte[] value() {
            return this.value;
        }
    }

    static enum WritableMetadata {
        HEADERS("headers", (DataType)DataTypes.MAP((DataType)DataTypes.STRING().nullable(), (DataType)DataTypes.BYTES().nullable()).nullable(), new MetadataConverter() {
            private static final long serialVersionUID = 1L;

            public Object read(RowData row, int pos) {
                if (row.isNullAt(pos)) {
                    return null;
                } else {
                    MapData map = row.getMap(pos);
                    ArrayData keyArray = map.keyArray();
                    ArrayData valueArray = map.valueArray();
                    List<Header> headers = new ArrayList();

                    for(int i = 0; i < keyArray.size(); ++i) {
                        if (!keyArray.isNullAt(i) && !valueArray.isNullAt(i)) {
                            String key = keyArray.getString(i).toString();
                            byte[] value = valueArray.getBinary(i);
                            headers.add(new KafkaDynamicSinkTest.KafkaHeader(key, value));
                        }
                    }

                    return headers;
                }
            }
        }),
        TIMESTAMP("timestamp", (DataType)DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE(3).nullable(), new MetadataConverter() {
            private static final long serialVersionUID = 1L;

            public Object read(RowData row, int pos) {
                return row.isNullAt(pos) ? null : row.getTimestamp(pos, 3).getMillisecond();
            }
        });

        final String key;
        final DataType dataType;
        final MetadataConverter converter;

        private WritableMetadata(String key, DataType dataType, MetadataConverter converter) {
            this.key = key;
            this.dataType = dataType;
            this.converter = converter;
        }
    }

    private static class TriggerSinkFunction
            extends KeyedCoProcessFunction<String, RowDataWithKey, RowDataWithKey, RowData> {

        private MapState<String, RowData> latestRecord;

        private MapStateDescriptor<String, RowData> latestRecordDescriptor = new MapStateDescriptor<String, RowData>("latestRecord", String.class, RowData.class);;

        @Override
        public void open(Configuration parameters) {
            latestRecord = getRuntimeContext().getMapState(latestRecordDescriptor);
        }


        @Override
        public void processElement1(RowDataWithKey value, Context ctx, Collector<RowData> out) throws Exception {
            latestRecord.put(value.getKey(), value.getValue());
            out.collect(value.getValue());
        }

        @Override
        public void processElement2(RowDataWithKey value, Context ctx, Collector<RowData> out) throws Exception {
            if (latestRecord.contains(value.getKey())) {
                out.collect(latestRecord.get(value.getKey()));
            } else {
                value.getValue().setRowKind(RowKind.DELETE);
                out.collect(value.getValue());
            }
        }
    }
}
