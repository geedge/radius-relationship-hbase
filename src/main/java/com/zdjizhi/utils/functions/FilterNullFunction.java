package com.zdjizhi.utils.functions;

import cn.hutool.log.Log;
import cn.hutool.log.LogFactory;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.java.tuple.Tuple6;

public class FilterNullFunction implements FilterFunction<Tuple6<String,String,String,String,Long,Integer>> {
    private static final Log logger = LogFactory.get();

    @Override
    public boolean filter(Tuple6<String,String,String,String,Long,Integer> message) {
        boolean isFilter = false;
        try {
            if (!"".equals(message.f0)) {
                 isFilter = true;
            }
          } catch (RuntimeException re) {
            logger.error("Radius日志条件过滤异常,异常信息为：" + re);
        }
        return isFilter;
    }
}

