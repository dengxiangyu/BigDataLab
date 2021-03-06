package com.fox.bigtata.experiment.flink.apitest.source;
import com.fox.bigtata.experiment.flink.beans.SensorReading;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.table.expressions.Rand;

import java.util.HashMap;
import java.util.Random;
/**
 * 自定义数据源
 */
public class SourecTest_user_defined {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        //从自定义的数据源读取数据
        DataStream<SensorReading> dataStream = env.addSource(new MySensorSource());
        //打印输出
        dataStream.print();
        env.execute();
    }

    //实现自定义的SourceFunction
    public static class MySensorSource implements SourceFunction<SensorReading> {
        //定义一个 标志  ，用来控制数据的产生
        private boolean running = true;

        @Override
        public void run(SourceContext<SensorReading> ctx) throws Exception {
            //定义一个随机数发生器
            Random radom = new Random();
            //设置10个传感器的初始温度
            HashMap<String, Double> sensorTempMap = new HashMap<>();
            for (int i = 0; i < 10; i++) {
                sensorTempMap.put("sensor_" + (i + 1), 60 + radom.nextGaussian() * 20);
            }
            while (running) {
                //更新传感器温度
                for (String sensoId : sensorTempMap.keySet()) {
                    //在当前温度基础上随机波动经
                    Double newTemp = sensorTempMap.get(sensoId) + radom.nextGaussian();
                    sensorTempMap.put(sensoId, newTemp);
                    ctx.collect(new SensorReading(sensoId, System.currentTimeMillis(), newTemp));
                }
                //控制输出频率
                Thread.sleep(1000L);
            }
        }

        @Override
        public void cancel() {
            running = false;
        }
    }
}