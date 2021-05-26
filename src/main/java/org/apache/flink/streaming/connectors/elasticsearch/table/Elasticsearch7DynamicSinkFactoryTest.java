//
// Source code recreated from a .class file by IntelliJ IDEA
// (powered by Fernflower decompiler)
//

package org.apache.flink.streaming.connectors.elasticsearch.table;

import java.util.Optional;
import java.util.Set;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.streaming.connectors.kafka.table.KafkaOptions;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.connector.format.DecodingFormat;
import org.apache.flink.table.connector.format.EncodingFormat;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.factories.DeserializationFormatFactory;
import org.apache.flink.table.factories.DynamicTableSinkFactory;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.factories.SerializationFormatFactory;
import org.apache.flink.table.factories.DynamicTableFactory.Context;
import org.apache.flink.table.factories.FactoryUtil.TableFactoryHelper;
import org.apache.flink.table.utils.TableSchemaUtils;
import org.apache.flink.types.RowKind;
import org.apache.flink.util.StringUtils;

@Internal
public class Elasticsearch7DynamicSinkFactoryTest implements DynamicTableSinkFactory {
    private static final Set<ConfigOption<?>> requiredOptions;
    private static final Set<ConfigOption<?>> optionalOptions;

    public Elasticsearch7DynamicSinkFactoryTest() {
    }

    public DynamicTableSink createDynamicTableSink(Context context) {
        TableSchema tableSchema = context.getCatalogTable().getSchema();
        ElasticsearchValidationUtils.validatePrimaryKey(tableSchema);
        TableFactoryHelper helper = FactoryUtil.createTableFactoryHelper(this, context);
        EncodingFormat<SerializationSchema<RowData>> format = helper.discoverEncodingFormat(SerializationFormatFactory.class, ElasticsearchOptions.FORMAT_OPTION);
        Optional<DecodingFormat<DeserializationSchema<RowData>>> keyDecodingFormat = getKeyDecodingFormat(helper);
        helper.validate();
        Configuration configuration = new Configuration();
        context.getCatalogTable().getOptions().forEach(configuration::setString);
        Elasticsearch7Configuration config = new Elasticsearch7Configuration(configuration, context.getClassLoader());
        this.validate(config, configuration);
        return new Elasticsearch7DynamicSink(format, config, TableSchemaUtils.getPhysicalSchema(tableSchema));
    }

    private void validate(Elasticsearch7Configuration config, Configuration originalConfiguration) {
        config.getFailureHandler();
        config.getHosts();
        validate(config.getIndex().length() >= 1, () -> {
            return String.format("'%s' must not be empty", ElasticsearchOptions.INDEX_OPTION.key());
        });
        int maxActions = config.getBulkFlushMaxActions();
        validate(maxActions == -1 || maxActions >= 1, () -> {
            return String.format("'%s' must be at least 1. Got: %s", ElasticsearchOptions.BULK_FLUSH_MAX_ACTIONS_OPTION.key(), maxActions);
        });
        long maxSize = config.getBulkFlushMaxByteSize();
        long mb1 = 1048576L;
        validate(maxSize == -1L || maxSize >= mb1 && maxSize % mb1 == 0L, () -> {
            return String.format("'%s' must be in MB granularity. Got: %s", ElasticsearchOptions.BULK_FLASH_MAX_SIZE_OPTION.key(), ((MemorySize)originalConfiguration.get(ElasticsearchOptions.BULK_FLASH_MAX_SIZE_OPTION)).toHumanReadableString());
        });
        validate((Boolean)config.getBulkFlushBackoffRetries().map((retries) -> {
            return retries >= 1;
        }).orElse(true), () -> {
            return String.format("'%s' must be at least 1. Got: %s", ElasticsearchOptions.BULK_FLUSH_BACKOFF_MAX_RETRIES_OPTION.key(), config.getBulkFlushBackoffRetries().get());
        });
        if (config.getUsername().isPresent() && !StringUtils.isNullOrWhitespaceOnly((String)config.getUsername().get())) {
            validate(config.getPassword().isPresent() && !StringUtils.isNullOrWhitespaceOnly((String)config.getPassword().get()), () -> {
                return String.format("'%s' and '%s' must be set at the same time. Got: username '%s' and password '%s'", ElasticsearchOptions.USERNAME_OPTION.key(), ElasticsearchOptions.PASSWORD_OPTION.key(), config.getUsername().get(), config.getPassword().orElse(""));
            });
        }

    }

    private static void validate(boolean condition, Supplier<String> message) {
        if (!condition) {
            throw new ValidationException((String)message.get());
        }
    }

    public String factoryIdentifier() {
        return "elasticsearch-7";
    }

    public Set<ConfigOption<?>> requiredOptions() {
        return requiredOptions;
    }

    public Set<ConfigOption<?>> optionalOptions() {
        return optionalOptions;
    }

    static {
        requiredOptions = (Set)Stream.of(ElasticsearchOptions.HOSTS_OPTION, ElasticsearchOptions.INDEX_OPTION).collect(Collectors.toSet());
        optionalOptions = (Set)Stream.of(ElasticsearchOptions.KEY_DELIMITER_OPTION, ElasticsearchOptions.FAILURE_HANDLER_OPTION, ElasticsearchOptions.FLUSH_ON_CHECKPOINT_OPTION, ElasticsearchOptions.BULK_FLASH_MAX_SIZE_OPTION, ElasticsearchOptions.BULK_FLUSH_MAX_ACTIONS_OPTION, ElasticsearchOptions.BULK_FLUSH_INTERVAL_OPTION, ElasticsearchOptions.BULK_FLUSH_BACKOFF_TYPE_OPTION, ElasticsearchOptions.BULK_FLUSH_BACKOFF_MAX_RETRIES_OPTION, ElasticsearchOptions.BULK_FLUSH_BACKOFF_DELAY_OPTION, ElasticsearchOptions.CONNECTION_MAX_RETRY_TIMEOUT_OPTION, ElasticsearchOptions.CONNECTION_PATH_PREFIX, ElasticsearchOptions.FORMAT_OPTION, ElasticsearchOptions.PASSWORD_OPTION, ElasticsearchOptions.USERNAME_OPTION).collect(Collectors.toSet());
    }

    private static Optional<DecodingFormat<DeserializationSchema<RowData>>> getKeyDecodingFormat(TableFactoryHelper helper) {
        Optional<DecodingFormat<DeserializationSchema<RowData>>> keyDecodingFormat = helper.discoverOptionalDecodingFormat(DeserializationFormatFactory.class, KafkaOptions.KEY_FORMAT);
        keyDecodingFormat.ifPresent((format) -> {
            if (!format.getChangelogMode().containsOnly(RowKind.INSERT)) {
                throw new ValidationException(String.format("A key format should only deal with INSERT-only records. But %s has a changelog mode of %s.", helper.getOptions().get(KafkaOptions.KEY_FORMAT), format.getChangelogMode()));
            }
        });
        return keyDecodingFormat;
    }
}
