package com.zdjizhi.utils.functions;

import cn.hutool.log.Log;
import cn.hutool.log.LogFactory;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;

import static com.zdjizhi.utils.hbase.HBaseUtils.dataValidation;

/**
 * @author qidaijie
 * @Package com.zdjizhi.utils.functions
 * @Description:
 * @date 2021/5/2715:01
 */
public class GetAccountMapFunction implements MapFunction<Tuple2<String, String>, Tuple2<String, String>> {
    private static final Log logger = LogFactory.get();


    @Override
    public Tuple2<String, String> map(Tuple2<String, String> stringStringTuple2) throws Exception {
        try {
            String framedIp = stringStringTuple2.f0;
            String account = stringStringTuple2.f1;
            boolean validation = dataValidation(framedIp, account);
            if (validation) {
                return Tuple2.of(framedIp, account);
            } else {
                return Tuple2.of("", "");
            }
        } catch (RuntimeException e) {
            logger.error("解析Radius数据获取用户信息异常,异常信息：" + e);
        }
        return Tuple2.of("", "");    }
}
