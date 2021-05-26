//
// Source code recreated from a .class file by IntelliJ IDEA
// (powered by Fernflower decompiler)
//

package org.apache.flink.streaming.connectors.elasticsearch.table;

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.connectors.elasticsearch7.ElasticsearchSink;
import org.apache.flink.streaming.connectors.elasticsearch7.ElasticsearchSink.Builder;
import org.apache.flink.streaming.connectors.elasticsearch7.RestClientFactory;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.format.EncodingFormat;
import org.apache.flink.table.connector.sink.DataStreamSinkProvider;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.data.RowData;
import org.apache.flink.types.RowKind;
import org.apache.flink.util.StringUtils;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.common.xcontent.XContentType;

import javax.annotation.Nullable;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.Properties;

@Internal
final class Elasticsearch7DynamicSinkTest implements DynamicTableSink {
    @VisibleForTesting
    static final Elasticsearch7DynamicSinkTest.Elasticsearch7RequestFactory REQUEST_FACTORY = new Elasticsearch7DynamicSinkTest.Elasticsearch7RequestFactory();
    private final EncodingFormat<SerializationSchema<RowData>> format;
    private final TableSchema schema;
    private final Elasticsearch7Configuration config;
    private final Elasticsearch7DynamicSinkTest.ElasticSearchBuilderProvider builderProvider;

    public Elasticsearch7DynamicSinkTest(EncodingFormat<SerializationSchema<RowData>> format, Elasticsearch7Configuration config, TableSchema schema) {
        this(format, config, schema, Builder::new);
    }

    Elasticsearch7DynamicSinkTest(EncodingFormat<SerializationSchema<RowData>> format, Elasticsearch7Configuration config, TableSchema schema, Elasticsearch7DynamicSinkTest.ElasticSearchBuilderProvider builderProvider) {
        this.format = format;
        this.schema = schema;
        this.config = config;
        this.builderProvider = builderProvider;
    }

    public ChangelogMode getChangelogMode(ChangelogMode requestedMode) {
        org.apache.flink.table.connector.ChangelogMode.Builder builder = ChangelogMode.newBuilder();
        Iterator var3 = requestedMode.getContainedKinds().iterator();

        while(var3.hasNext()) {
            RowKind kind = (RowKind)var3.next();
            if (kind != RowKind.UPDATE_BEFORE) {
                builder.addContainedKind(kind);
            }
        }

        return builder.build();
    }

    @Override
    public SinkRuntimeProvider getSinkRuntimeProvider(Context context) {
        return new DataStreamSinkProvider() {
            @Override
            public DataStreamSink<?> consumeDataStream(DataStream<RowData> dataStream) {
                SinkFunction<RowData> originalSink = getESSinkFunction(context);

                Properties properties = new Properties();
                properties.setProperty("bootstrap.servers", "b-3.test.v96qm6.c13.kafka.us-east-1.amazonaws.com:9094,b-1.test.v96qm6.c13.kafka.us-east-1.amazonaws.com:9094,b-2.test.v96qm6.c13.kafka.us-east-1.amazonaws.com:9094");
                properties.setProperty("security.protocol", "SSL");
                properties.setProperty("ssl.truststore.location", "/usr/lib/jvm/java-1.8.0-amazon-corretto.x86_64/jre/lib/security/cacerts");
                SerializationSchema<RowData> esFormat = (SerializationSchema)format.createRuntimeEncoder(context, schema.toRowDataType());

                FlinkKafkaProducer<RowData> myProducer = new FlinkKafkaProducer<>(
                        "test2",                  // target topic
                        esFormat,    // serialization schema
                        properties); // fault-tolerance

                dataStream.addSink(myProducer);
                return dataStream.addSink(originalSink);
            }
        };
    }

    private SinkFunction<RowData> getESSinkFunction(Context context) {
        SerializationSchema<RowData> format = (SerializationSchema)this.format.createRuntimeEncoder(context, this.schema.toRowDataType());
        RowElasticsearchSinkFunction upsertFunction = new RowElasticsearchSinkFunction(IndexGeneratorFactory.createIndexGenerator(this.config.getIndex(), this.schema), (String)null, format, XContentType.JSON, REQUEST_FACTORY, KeyExtractor.createKeyExtractor(this.schema, this.config.getKeyDelimiter()));
        Builder<RowData> builder = this.builderProvider.createBuilder(this.config.getHosts(), upsertFunction);
        builder.setFailureHandler(this.config.getFailureHandler());
        builder.setBulkFlushMaxActions(this.config.getBulkFlushMaxActions());
        builder.setBulkFlushMaxSizeMb((int)(this.config.getBulkFlushMaxByteSize() >> 20));
        builder.setBulkFlushInterval(this.config.getBulkFlushInterval());
        builder.setBulkFlushBackoff(this.config.isBulkFlushBackoffEnabled());
        this.config.getBulkFlushBackoffType().ifPresent(builder::setBulkFlushBackoffType);
        this.config.getBulkFlushBackoffRetries().ifPresent(builder::setBulkFlushBackoffRetries);
        this.config.getBulkFlushBackoffDelay().ifPresent(builder::setBulkFlushBackoffDelay);
        if (this.config.getUsername().isPresent() && this.config.getPassword().isPresent() && !StringUtils.isNullOrWhitespaceOnly((String)this.config.getUsername().get()) && !StringUtils.isNullOrWhitespaceOnly((String)this.config.getPassword().get())) {
            builder.setRestClientFactory(new Elasticsearch7DynamicSinkTest.AuthRestClientFactory((String)this.config.getPathPrefix().orElse(null), (String)this.config.getUsername().get(), (String)this.config.getPassword().get()));
        } else {
            builder.setRestClientFactory(new Elasticsearch7DynamicSinkTest.DefaultRestClientFactory((String)this.config.getPathPrefix().orElse(null)));
        }

        ElasticsearchSink<RowData> sink = builder.build();
        if (this.config.isDisableFlushOnCheckpoint()) {
            sink.disableFlushOnCheckpoint();
        }

        return sink;
    }

    public DynamicTableSink copy() {
        return this;
    }

    public String asSummaryString() {
        return "Elasticsearch7";
    }

    public boolean equals(Object o) {
        if (this == o) {
            return true;
        } else if (o != null && this.getClass() == o.getClass()) {
            Elasticsearch7DynamicSinkTest that = (Elasticsearch7DynamicSinkTest)o;
            return Objects.equals(this.format, that.format) && Objects.equals(this.schema, that.schema) && Objects.equals(this.config, that.config) && Objects.equals(this.builderProvider, that.builderProvider);
        } else {
            return false;
        }
    }

    public int hashCode() {
        return Objects.hash(new Object[]{this.format, this.schema, this.config, this.builderProvider});
    }

    private static class Elasticsearch7RequestFactory implements RequestFactory {
        private Elasticsearch7RequestFactory() {
        }

        public UpdateRequest createUpdateRequest(String index, String docType, String key, XContentType contentType, byte[] document) {
            return (new UpdateRequest(index, key)).doc(document, contentType).upsert(document, contentType);
        }

        public IndexRequest createIndexRequest(String index, String docType, String key, XContentType contentType, byte[] document) {
            return (new IndexRequest(index)).id(key).source(document, contentType);
        }

        public DeleteRequest createDeleteRequest(String index, String docType, String key) {
            return new DeleteRequest(index, key);
        }
    }

    @VisibleForTesting
    static class AuthRestClientFactory implements RestClientFactory {
        private final String pathPrefix;
        private final String username;
        private final String password;
        private transient CredentialsProvider credentialsProvider;

        public AuthRestClientFactory(@Nullable String pathPrefix, String username, String password) {
            this.pathPrefix = pathPrefix;
            this.password = password;
            this.username = username;
        }

        public void configureRestClientBuilder(RestClientBuilder restClientBuilder) {
            if (this.pathPrefix != null) {
                restClientBuilder.setPathPrefix(this.pathPrefix);
            }

            if (this.credentialsProvider == null) {
                this.credentialsProvider = new BasicCredentialsProvider();
                this.credentialsProvider.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials(this.username, this.password));
            }

            restClientBuilder.setHttpClientConfigCallback((httpAsyncClientBuilder) -> {
                return httpAsyncClientBuilder.setDefaultCredentialsProvider(this.credentialsProvider);
            });
        }

        public boolean equals(Object o) {
            if (this == o) {
                return true;
            } else if (o != null && this.getClass() == o.getClass()) {
                Elasticsearch7DynamicSinkTest.AuthRestClientFactory that = (Elasticsearch7DynamicSinkTest.AuthRestClientFactory)o;
                return Objects.equals(this.pathPrefix, that.pathPrefix) && Objects.equals(this.username, that.username) && Objects.equals(this.password, that.password);
            } else {
                return false;
            }
        }

        public int hashCode() {
            return Objects.hash(new Object[]{this.pathPrefix, this.password, this.username});
        }
    }

    @VisibleForTesting
    static class DefaultRestClientFactory implements RestClientFactory {
        private final String pathPrefix;

        public DefaultRestClientFactory(@Nullable String pathPrefix) {
            this.pathPrefix = pathPrefix;
        }

        public void configureRestClientBuilder(RestClientBuilder restClientBuilder) {
            if (this.pathPrefix != null) {
                restClientBuilder.setPathPrefix(this.pathPrefix);
            }

        }

        public boolean equals(Object o) {
            if (this == o) {
                return true;
            } else if (o != null && this.getClass() == o.getClass()) {
                Elasticsearch7DynamicSinkTest.DefaultRestClientFactory that = (Elasticsearch7DynamicSinkTest.DefaultRestClientFactory)o;
                return Objects.equals(this.pathPrefix, that.pathPrefix);
            } else {
                return false;
            }
        }

        public int hashCode() {
            return Objects.hash(new Object[]{this.pathPrefix});
        }
    }

    @FunctionalInterface
    interface ElasticSearchBuilderProvider {
        Builder<RowData> createBuilder(List<HttpHost> var1, RowElasticsearchSinkFunction var2);
    }
}
