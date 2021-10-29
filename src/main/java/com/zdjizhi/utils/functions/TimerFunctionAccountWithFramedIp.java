package com.zdjizhi.utils.functions;


import com.zdjizhi.common.RadiusRelationshipConfig;
import com.zdjizhi.utils.StringUtil;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.zdjizhi.utils.hbase.HBaseUtils.insertData;

/**
 * @author qidaijie
 * @Package com.zdjizhi.utils.functions
 * @Description:
 * @date 2021/6/2316:59
 */
public class TimerFunctionAccountWithFramedIp extends KeyedProcessFunction<String, Tuple2<String, String>, Object> {
    private static final Logger logger = LoggerFactory.getLogger(TimerFunctionAccountWithFramedIp.class);
    private static Map<String,String> map = new HashMap<String,String>();
    private static List<Put> putList = new ArrayList<>();
    private static boolean first = true;


    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<Object> out) throws Exception {

            for(Map.Entry<String,String>  entry: map.entrySet() ){

                Put put = new Put(entry.getKey().getBytes());
                put.addColumn("radius".getBytes(), "framed_ip".getBytes(), entry.getValue().getBytes());
                put.addColumn("radius".getBytes(), "last_found_time".getBytes(), Bytes.toBytes(timestamp/1000));


                if(putList.size()<100000){
                    putList.add(put);
                }
                else{
                    insertData(putList,RadiusRelationshipConfig.HBASE_ACCOUNT_TABLE_NAME);
                    putList.clear();
                }
            }
        if(putList.size()>0) {
            insertData(putList, RadiusRelationshipConfig.HBASE_ACCOUNT_TABLE_NAME);
            putList.clear();
        }
        map.clear();
        ctx.timerService().registerProcessingTimeTimer(timestamp + (RadiusRelationshipConfig.HBASE_TICK_TUPLE_FREQ_SECS * 1000));
    }

    @Override
    public void processElement(Tuple2<String, String> value, Context ctx, Collector<Object> out) throws Exception {
        //仅在该算子接收到第一个数据时，注册一个定时器
        if (first) {
            first = false;
            Long time = System.currentTimeMillis();
            ctx.timerService().registerProcessingTimeTimer(time + (RadiusRelationshipConfig.HBASE_TICK_TUPLE_FREQ_SECS * 1000));
        }
        String account = value.f1;

        String framedIp = value.f0;
        if (StringUtil.isNotBlank(framedIp) && StringUtil.isNotBlank(account)) {
            map.put(account,framedIp);
        }
    }
}
