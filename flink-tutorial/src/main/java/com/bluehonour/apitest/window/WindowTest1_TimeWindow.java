package com.bluehonour.apitest.window;

import com.bluehonour.apitest.beans.SensorReading;
import org.apache.commons.collections.IteratorUtils;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.EventTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

public class WindowTest1_TimeWindow {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // socket文本流
        DataStreamSource<String> inputStream = env.socketTextStream("master", 7777);

        // 转换成SensorReading类型
        DataStream<SensorReading> dataStream = inputStream.map(line -> {
            String[] fields = line.split(",");
            return new SensorReading(fields[0], new Long(fields[1]), new Double(fields[2]));
        });

        //开窗测试

        //1.增量聚合函数
        DataStream<Integer> resultStream = dataStream.keyBy("id")
//                .countWindow(5,2)
//        .window(EventTimeSessionWindows.withGap(Time.seconds(10)))
//        .window(TumblingProcessingTimeWindows.of(Time.seconds(10)))
//                .timeWindow(Time.seconds(10), Time.seconds(3))
                .timeWindow(Time.seconds(10))
                .aggregate(new AggregateFunction<SensorReading, Integer, Integer>() {
                    @Override
                    public Integer createAccumulator() {
                        return 0;
                    }

                    @Override
                    public Integer add(SensorReading value, Integer accumulator) {
                        return accumulator + 1;
                    }

                    @Override
                    public Integer getResult(Integer accumulator) {
                        return accumulator;
                    }

                    @Override
                    public Integer merge(Integer a, Integer b) {
                        return a + b;
                    }
                });

//        resultStream.print();

        //2.全窗口函数
        DataStream<Tuple3<String, Long, Integer>> resultStream2 = dataStream.keyBy("id")
                .timeWindow(Time.seconds(10))
                .process(new ProcessWindowFunction<SensorReading, Tuple3<String, Long, Integer>, Tuple, TimeWindow>() {
                    @Override
                    public void process(Tuple tuple, Context context, Iterable<SensorReading> elements, Collector<Tuple3<String, Long, Integer>> out) throws Exception {
                        String id = tuple.getField(0);
                        long windowEnd = context.window().getEnd();
                        int count = IteratorUtils.toList(elements.iterator()).size();
                        out.collect(new Tuple3<>(id, windowEnd, count));
                    }
                });
//                .apply(new WindowFunction<SensorReading, Tuple3<String, Long, Integer>, Tuple, TimeWindow>() {
//
//                    @Override
//                    public void apply(Tuple tuple, TimeWindow window, Iterable<SensorReading> input, Collector<Tuple3<String, Long, Integer>> out) throws Exception {
//                        String id = tuple.getField(0);
//                        long windowEnd = window.getEnd();
//                        int count = IteratorUtils.toList(input.iterator()).size();
//                        out.collect(new Tuple3<>(id, windowEnd, count));
//                    }
//                }
//                );

//        resultStream2.print();

        // 3. 其它可选API
        OutputTag<SensorReading> outputTag = new OutputTag<SensorReading>("late") {
        };

        SingleOutputStreamOperator<SensorReading> sumStream = dataStream.keyBy("id")
                .timeWindow(Time.seconds(10))
//                .trigger()
//                .evictor()
                .allowedLateness(Time.seconds(5))
                .sideOutputLateData(outputTag)
                .sum("temperature");

        DataStream<SensorReading> lateStream = sumStream.getSideOutput(outputTag);
        lateStream.print("late");
        sumStream.print("normal");

        env.execute();

    }

}
