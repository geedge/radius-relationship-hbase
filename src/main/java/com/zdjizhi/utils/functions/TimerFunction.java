package com.zdjizhi.utils.functions;


import com.zdjizhi.common.RadiusRelationshipConfig;
import com.zdjizhi.utils.StringUtil;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.hadoop.hbase.client.Put;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

import static com.zdjizhi.utils.hbase.HBaseUtils.insertData;

/**
 * @author qidaijie
 * @Package com.zdjizhi.utils.functions
 * @Description:
 * @date 2021/6/2316:59
 */
public class TimerFunction extends KeyedProcessFunction<String, Tuple2<String, String>, Object> {
    private static final Logger logger = LoggerFactory.getLogger(TimerFunction.class);

    private static List<Put> putList = new ArrayList<>();
    private static boolean first = true;


    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<Object> out) throws Exception {
        if (putList.size() != 0) {
            insertData(putList);
            putList.clear();
        }
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
        String framedIp = value.f0;
        String account = value.f1;
        if (StringUtil.isNotBlank(framedIp) && StringUtil.isNotBlank(account)) {
            Put put = new Put(value.f0.getBytes());
            put.addColumn("subscriber_id".getBytes(), "account".getBytes(), value.f1.getBytes());
            putList.add(put);
        }
    }
}
