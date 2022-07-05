package com.zdjizhi.topology;

import cn.hutool.log.Log;
import cn.hutool.log.LogFactory;
import com.zdjizhi.common.RadiusRelationshipConfig;
import com.zdjizhi.utils.functions.FilterNullFunction;
import com.zdjizhi.utils.functions.ParseFunction;
import com.zdjizhi.utils.hbasepackage.HbaseSinkAccount;
import com.zdjizhi.utils.hbasepackage.HbaseSinkFramedip;
import com.zdjizhi.utils.kafka.Consumer;
import org.apache.flink.api.java.tuple.Tuple6;
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
public class RadiusRelation {
    private static final Log logger = LogFactory.get();


    public static void main(String[] args) {
        final StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<String> streamSource = environment.addSource(Consumer.getKafkaConsumer());

        DataStream<Tuple6<String,String,String,String,Long,Integer>> getObject = streamSource.map(new ParseFunction()).name("ParseJson");

        DataStream<Tuple6<String,String,String,String,Long,Integer>>  filterOriginalData = getObject.filter(new FilterNullFunction()).name("FilterOriginalData");

        KeyedStream<Tuple6<String,String,String,String,Long,Integer>, String> FrameipWithaccount = filterOriginalData.keyBy(value -> value.f0);

        KeyedStream<Tuple6<String,String,String,String,Long,Integer>, String> accountWithFrameip = filterOriginalData.keyBy(value -> value.f1);

        FrameipWithaccount.addSink(new HbaseSinkFramedip(RadiusRelationshipConfig.HBASE_ZOOKEEPER_SERVERS));

        accountWithFrameip.addSink(new HbaseSinkAccount(RadiusRelationshipConfig.HBASE_ZOOKEEPER_SERVERS));
        try {
            environment.execute("RADIUS-RELATIONSHIP-HBASE-V2");
        } catch (Exception e) {
            logger.error("This Flink task start ERROR! Exception information is :" + e);
        }

    }


}
