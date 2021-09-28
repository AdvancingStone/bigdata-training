package com.bluehonour.apitest.source;/**
 * Copyright (c) 2018-2028 尚硅谷 All Rights Reserved
 * <p>
 * Project: FlinkTutorial
 * Package: com.atguigu.apitest.source
 * Version: 1.0
 * <p>
 * Created by wushengran on 2020/11/7 11:48
 */

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @ClassName: SourceTest2_File
 * @Description:
 * @Author: wushengran on 2020/11/7 11:48
 * @Version: 1.0
 */
public class SourceTest2_File {
    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 从文件读取数据
        DataStreamSource<String> dataStream = env.readTextFile("D:\\git_project\\my\\bigdata-training\\flink-tutorial\\src\\main\\resources\\sensor.txt");

        // 打印输出
        dataStream.print();

        env.execute();
    }
}
