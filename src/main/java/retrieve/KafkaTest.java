package retrieve;

import org.apache.flink.table.api.Table;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.util.Collector;

import java.util.Properties;

public class KafkaTest {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment bsEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        final EnvironmentSettings bsSettings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        final StreamTableEnvironment bsTableEnv = StreamTableEnvironment.create(bsEnv, bsSettings);

        DataStream<String> dataStream = bsEnv.fromElements("Alice", "Bob", "John");

        Table inputTable = bsTableEnv.fromDataStream(dataStream);
        bsTableEnv.createTemporaryView("InputTable", inputTable);

        Table resultTable = bsTableEnv.sqlQuery("INSERT INTO SELECT * FROM InputTable");

        bsTableEnv.executeSql("CREATE TABLE pageviews_per_region (\n" +
                "  user_region STRING,\n" +
                "  pv BIGINT,\n" +
                "  uv BIGINT,\n" +
                "  PRIMARY KEY (user_region) NOT ENFORCED\n" +
                ") WITH (\n" +
                "  'connector' = 'upsert-kafka',\n" +
                "  'topic' = 'pageviews_per_region',\n" +
                "  'properties.bootstrap.servers' = '...',\n" +
                "  'key.format' = 'avro',\n" +
                "  'value.format' = 'avro'\n" +
                ");");


        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "b-3.test.v96qm6.c13.kafka.us-east-1.amazonaws.com:9094,b-1.test.v96qm6.c13.kafka.us-east-1.amazonaws.com:9094,b-2.test.v96qm6.c13.kafka.us-east-1.amazonaws.com:9094");
        properties.setProperty("security.protocol", "SSL");
        properties.setProperty("ssl.truststore.location", "/usr/lib/jvm/java-1.8.0-amazon-corretto.x86_64/jre/lib/security/cacerts");

        FlinkKafkaProducer<String> myProducer = new FlinkKafkaProducer<>(
                "test2",                  // target topic
                new SimpleStringSchema(),    // serialization schema
                properties); // fault-tolerance

        Properties consumerProperties = new Properties();
        consumerProperties.setProperty("bootstrap.servers", "b-3.test.v96qm6.c13.kafka.us-east-1.amazonaws.com:9094,b-1.test.v96qm6.c13.kafka.us-east-1.amazonaws.com:9094,b-2.test.v96qm6.c13.kafka.us-east-1.amazonaws.com:9094");
        consumerProperties.setProperty("security.protocol", "SSL");
        consumerProperties.setProperty("ssl.truststore.location", "/usr/lib/jvm/java-1.8.0-amazon-corretto.x86_64/jre/lib/security/cacerts");
//        DataStream<String> consumer = env
//                .addSource(new FlinkKafkaConsumer<>("test3", new SimpleStringSchema(), consumerProperties));

//        consumer.addSink(myProducer);
    }

    public static class Splitter implements FlatMapFunction<String, String> {
        @Override
        public void flatMap(String sentence, Collector<String> out) throws Exception {
            for (String word: sentence.split(" ")) {
                out.collect(word);
            }
        }
    }
}
