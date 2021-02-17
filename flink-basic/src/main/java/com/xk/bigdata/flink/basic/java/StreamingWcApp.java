package com.xk.bigdata.flink.basic.java;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * Java Flink 流处理 WC
 */
public class StreamingWcApp {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.socketTextStream("bigdata", 16666)
                .flatMap(new FlatMapFunction<String, String>() {
                    @Override
                    public void flatMap(String s, Collector<String> collector) throws Exception {
                        String[] words = s.toLowerCase().split(",");
                        for (String word : words) {
                            collector.collect(word);
                        }
                    }
                })
                .map(new MapFunction<String, Tuple2<String, Integer>>() {
                    @Override
                    public Tuple2<String, Integer> map(String s) throws Exception {
                        return new Tuple2<>(s, 1);
                    }
                })
                .keyBy(0)
                .sum(1)
                .print();

        env.execute(StreamingWcApp.class.getSimpleName());

    }

}
