package com.fox.bigtata.experiment.flink.wc;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.lang.reflect.Parameter;
import java.util.ArrayList;
import java.util.List;

/**
 * @ClassName: StreamWordCount
 * @Description:
 * @Author:
 * @Version: 1.0
 */
public class StreamWordCount {
    public static void main(String[] args) throws Exception {
    //创建流处理执行环境
        StreamExecutionEnvironment env =  StreamExecutionEnvironment.getExecutionEnvironment();

//        // 从文件中读取数据
//        String inputPath= "/Users/shuidi/soft/testdata/flinkwsfile";
//        DataStream<String> inputDataStream = env.readTextFile(inputPath);

        // 用parameter tool 工具从持续启动参数中提取配置项
        ParameterTool parameterTool = ParameterTool.fromArgs(args);
        String host=parameterTool.get("host");
        int port=parameterTool.getInt("port");
        System.out.println("host:"+host+" port:"+port);
        // 从socket文本流读取数据
        //可以在linux or  mac 中用命令 nc -lk 端口 模拟持续监听该端口 ，可以在命令行向这个端口不断输入数据模拟实时流
        //
       // nc -l port 临时监听TCP端口
        // nc -lk port 永久监听TCP端口
        //
        DataStream<String> inputDataStream = env.socketTextStream(host,port);

        //基于数据流进行转换计算
        DataStream<Tuple2<String,Integer>> resultStream=inputDataStream.flatMap(new WordCount.MyFlatMapper()).keyBy(0).sum(1);
        resultStream.print();
        //执行服务
        env.execute();
    }
}