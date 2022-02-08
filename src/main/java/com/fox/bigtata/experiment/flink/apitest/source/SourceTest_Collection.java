package com.fox.bigtata.experiment.flink.apitest.source;

import com.fox.bigtata.experiment.flink.beans.SensorReading;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Arrays;

public class SourceTest_Collection {

    public static void main(String[] args) throws Exception {
    //创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
       env.setParallelism(1);
        // 从集合中读取数据
        DataStream<SensorReading> dataStream=env.fromCollection(Arrays.asList(
                new SensorReading("sensor_1",1547718199L,25.8),
                new SensorReading("sensor_2",1547718134L,35.8),
                new SensorReading("sensor_3",1547718154L,35.8),
                new SensorReading("sensor_4",1547718459L,35.8)

        ));
        DataStream<Integer> integerDataStream =env.fromElements(1,2,3,4,5);
        //打印输出
        dataStream.print("data");
        integerDataStream.print("int");
        //执行
        env.execute();
    }
}
