package com.zdjizhi.topology;

import cn.hutool.log.Log;
import cn.hutool.log.LogFactory;
import com.zdjizhi.utils.functions.*;

import com.zdjizhi.utils.kafka.Consumer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author qidaijie
 * @Package com.zdjizhi.topology
 * @Description:
 * @date 2021/5/2016:42
 */
public class RadiusRelationshipTopology {
    private static final Log logger = LogFactory.get();


    public static void main(String[] args) {
        final StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();

//        environment.enableCheckpointing(5000);

        DataStreamSource<String> streamSource = environment.addSource(Consumer.getKafkaConsumer());

        DataStream<String> filterOriginalData = streamSource.filter(new FilterNullFunction()).name("FilterOriginalData");

        DataStream<Tuple2<String, String>> getObject = filterOriginalData.map(new ParseFunction()).name("ParseJson");

        DataStream<Tuple2<String, String>> getRadiusAccount = getObject.map(new GetAccountMapFunction()).name("GetRadiusAccount");

        KeyedStream<Tuple2<String, String>, String> tuple2StringKeyedStream = getRadiusAccount.keyBy(value -> value.f0);

        KeyedStream<Tuple2<String, String>, String> accountWithFrameip = getObject.keyBy(value -> value.f1);

        tuple2StringKeyedStream.process(new TimerFunction()).name("UpdateHBase");

        accountWithFrameip.process(new TimerFunctionAccountWithFramedIp()).name("UpdateAccountHBase");

        try {
            environment.execute("RADIUS-RELATIONSHIP-HBASE");
        } catch (Exception e) {
            logger.error("This Flink task start ERROR! Exception information is :" + e);
        }

    }


}
