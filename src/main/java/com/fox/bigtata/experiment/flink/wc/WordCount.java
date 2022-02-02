package com.fox.bigtata.experiment.flink.wc;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.util.Collector;
/**
 * @ClassName: WordCount
 * @Description:
 * @Author: fox on 2022/02/6 11:22
 * @Version: 1.0
 */
//批处理 word count
public class WordCount {
    public static void main(String[] args) throws Exception{
        // 创建执行环境
        ExecutionEnvironment env =  ExecutionEnvironment.getExecutionEnvironment();
        // 从文件中读取数据
        String inputPath= "/Users/shuidi/soft/testdata/flinkwsfile";
        DataSet<String> inputDataSet= env.readTextFile(inputPath);
//        inputDataSet.print();
          //对数据集进行处理，按空格分词展开，转换成(word,1)这样的二元组进行统计
        DataSet<Tuple2<String,Integer>> resultSet = inputDataSet.flatMap(new MyFlatMapper()).groupBy(0).sum(1); //先按照第一个位置的word分组 然后将第二个位置上的数据求和
        resultSet.print();

    }
    // 自定义累，实现FlatMapFunction接口
    public static class  MyFlatMapper implements FlatMapFunction<String,Tuple2<String,Integer>>{
        @Override
        public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
            //按空格分词
            String [] words = value.split(" ");
            //遍历所有word，包成二元组输出
            for(String word:words){
                out.collect(new Tuple2<>(word,1));
            }
        }
    }

}
