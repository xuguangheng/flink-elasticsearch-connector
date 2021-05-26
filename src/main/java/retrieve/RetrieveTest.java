package retrieve;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction;
import org.apache.flink.util.Collector;


public class RetrieveTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<Tuple2<String, Integer>> dataStream = env
                .socketTextStream("localhost", 9999)
                .flatMap(new Splitter()).keyBy(value -> value.f0);

        DataStream<Tuple2<String, Integer>> dataStream2 = env
                .socketTextStream("localhost", 8888)
                .flatMap(new Splitter()).keyBy(value -> value.f0);


        dataStream.connect(dataStream2).process(new CountWithTimeoutFunction()).print();

        env.execute("Window WordCount");
    }

    public static class Splitter implements FlatMapFunction<String, Tuple2<String, Integer>> {
        @Override
        public void flatMap(String sentence, Collector<Tuple2<String, Integer>> out) throws Exception {
            for (String word: sentence.split(" ")) {
                out.collect(new Tuple2<String, Integer>(word, 1));
            }
        }
    }

    public static class CountWithTimeoutFunction
            extends KeyedCoProcessFunction<String, Tuple2<String, Integer>, Tuple2<String, Integer>, Integer> {

        private MapState<String, Integer> latestCountMap;

        private MapStateDescriptor<String, Integer> countMapStateDescriptor;

        @Override
        public void open(Configuration parameters) {
            countMapStateDescriptor = new MapStateDescriptor<String, Integer>("countMap", String.class, Integer.class);
            latestCountMap = getRuntimeContext().getMapState(countMapStateDescriptor);
        }

        @Override
        public void processElement1(Tuple2<String, Integer> a, Context context, Collector<Integer> collector) throws Exception {
            if (latestCountMap.contains(a.f0)) {
                latestCountMap.put(a.f0, a.f1 + latestCountMap.get(a.f0));
            } else {
                latestCountMap.put(a.f0, a.f1);
            }
        }

        @Override
        public void processElement2(Tuple2<String, Integer> b, Context context, Collector<Integer> collector) throws Exception {
            if (latestCountMap.contains(b.f0)) {
                collector.collect(b.f1 + latestCountMap.get(b.f0));
            }
        }
    }
}
