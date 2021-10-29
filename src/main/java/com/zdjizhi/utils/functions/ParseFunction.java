package com.zdjizhi.utils.functions;

import cn.hutool.log.Log;
import cn.hutool.log.LogFactory;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;

/**
 * @author qidaijie
 * @Package com.zdjizhi.utils.functions
 * @Description:
 * @date 2021/5/2715:01
 */

public class ParseFunction implements MapFunction<String, Tuple2<String, String>> {
    private static final Log logger = LogFactory.get();


    @Override
    public Tuple2<String, String> map(String logs) {
        try {
            JSONObject jsonObject = JSONObject.parseObject(logs);
            String framedIp = jsonObject.getString("radius_framed_ip");
            String account = jsonObject.getString("radius_account");

            return Tuple2.of(framedIp, account);

        } catch (RuntimeException e) {
            logger.error("解析Radius数据获取用户信息异常,异常信息：" + e);
        }
        return Tuple2.of("", "");
    }
}